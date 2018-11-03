#include <stdlib.h>
#include <tuple>
#include "common/redisreply.h"
#include "common/producertable.h"
#include "common/json.h"
#include "common/json.hpp"
#include "common/logger.h"
#include "common/redisapi.h"

using namespace std;
using json = nlohmann::json;

namespace swss {

ProducerTable::ProducerTable(DBConnector *db, const string &tableName)
    : ProducerTable(new RedisPipeline(db, 1), tableName, false)
{
    m_pipeowned = true;
}

ProducerTable::ProducerTable(RedisPipeline *pipeline, const string &tableName, bool buffered)
    : TableBase(pipeline->getDbId(), tableName)
    , TableName_KeyValueOpQueues(tableName)
    , m_buffered(buffered)
    , m_pipeowned(false)
    , m_pipe(pipeline)
{
    /*
     * KEYS[1] : tableName + "_KEY_VALUE_OP_QUEUE
     * ARGV[1] : key
     * ARGV[2] : value
     * ARGV[3] : op
     * KEYS[2] : tableName + "_CHANNEL"
     * ARGV[4] : "G"
     * ARGV[5] : number of HSET
     * ARGV[6] : number of HDEL
     * ARGV[7] : number of DEL
     */
    string luaEnque =
        "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2], ARGV[3])\n"
        "redis.call('PUBLISH', KEYS[2], ARGV[4])\n"
        "local hsetnum = tonumber(ARGV[5])\n"
        "local hdelnum = tonumber(ARGV[6])\n"
        "local delnum = tonumber(ARGV[7])\n"
        "for i = 1, hsetnum do\n"
        "    redis.call('HSET', ARGV[7+ i*3 -2], ARGV[7+ i*3 -1], ARGV[7+ i*3])\n"
        "end\n"
        "for i = 1, hdelnum do\n"
        "    redis.call('HDEL', ARGV[7+ hsetnum*3 + i*2 -1], ARGV[7+ hsetnum*3 + i*2])\n"
        "end\n"
        "for i = 1, delnum do\n"
        "    redis.call('DEL', ARGV[7+ hsetnum*3 + hdelnum*2 +i])\n"
        "end\n";

    m_shaEnque = m_pipe->loadRedisScript(luaEnque);
}

ProducerTable::ProducerTable(DBConnector *db, const string &tableName, const string &dumpFile)
    : ProducerTable(db, tableName)
{
    m_dumpFile.open(dumpFile, fstream::out | fstream::trunc);
    m_dumpFile << "[" << endl;
}

ProducerTable::~ProducerTable() {
    if (m_dumpFile.is_open())
    {
        m_dumpFile << endl << "]" << endl;
        m_dumpFile.close();
    }

    if (m_pipeowned)
    {
        delete m_pipe;
    }
}

void ProducerTable::setBuffered(bool buffered)
{
    m_buffered = buffered;
}

void ProducerTable::enqueueDbChange(const string &key, const string &value, const string &op, const string& /* prefix */,
                                    const std::vector<KeyOpFieldsValuesTuple> &vkco)
{
    vector<string> hsetKFV;
    vector<string> hdelKF;
    vector<string> DelK;
    for (const auto& kco: vkco)
    {
        if (kfvOp(kco) == "HMSET" || kfvOp(kco) == "HSET")
        {
            for (const auto& iv: kfvFieldsValues(kco))
            {
                hsetKFV.emplace_back(kfvKey(kco));
                hsetKFV.emplace_back(fvField(iv));
                hsetKFV.emplace_back(fvValue(iv));
            }
        }
        else if (kfvOp(kco) == "HDEL")
        {
            hdelKF.emplace_back(kfvKey(kco));
            hdelKF.emplace_back(fvField(kfvFieldsValues(kco)[0]));
        }
        else if (kfvOp(kco) == "DEL")
        {
            DelK.emplace_back(kfvKey(kco));
        }
        else
        {
            SWSS_LOG_ERROR("Unsupported redis op type %s",
                kfvOp(kco).c_str());
            throw system_error(make_error_code(errc::invalid_argument),
                "Unsupported redis op type");
        }
    }

    vector<string> args;
    args.emplace_back("EVALSHA");
    args.emplace_back(m_shaEnque);
    // Two keys: tableName + "_KEY_VALUE_OP_QUEUE; and tableName + "_CHANNEL"
    args.emplace_back("2");
    args.emplace_back(getKeyValueOpQueueTableName());
    args.emplace_back(getChannelName());

    // Four regular argvs
    args.emplace_back(key);
    args.emplace_back(value);
    args.emplace_back(op);
    args.emplace_back("G");

    // number of hset, hdel and del for argv[5], argv[6] and argv[7]
    args.emplace_back(to_string(hsetKFV.size()/3));
    args.emplace_back(to_string(hdelKF.size()/2));
    args.emplace_back(to_string(DelK.size()));

    for (const auto& i: hsetKFV)
    {
        args.emplace_back(i);
    }

    for (const auto& i: hdelKF)
    {
        args.emplace_back(i);
    }

    for (const auto& i: DelK)
    {
        args.emplace_back(i);
    }

    // Transform data structure
    vector<const char *> args1;
    transform(args.begin(), args.end(), back_inserter(args1), [](const string &s) { return s.c_str(); } );

    // Invoke redis command
    RedisCommand cmd;
    cmd.formatArgv((int)args1.size(), &args1[0], NULL);
    m_pipe->push(cmd, REDIS_REPLY_NIL);
}

void ProducerTable::set(const string &key, const vector<FieldValueTuple> &values, const string &op, const string &prefix,
                        const std::vector<KeyOpFieldsValuesTuple> &vkco)
{
    if (m_dumpFile.is_open())
    {
        if (!m_firstItem)
            m_dumpFile << "," << endl;
        else
            m_firstItem = false;

        json j;
        string json_key = getKeyName(key);
        j[json_key] = json::object();
        for (const auto &it : values)
            j[json_key][fvField(it)] = fvValue(it);
        j["OP"] = op;
        m_dumpFile << j.dump(4);
    }

    enqueueDbChange(key, JSon::buildJson(values), "S" + op, prefix, vkco);
    // Only buffer "set", "bulkset" or "create" operations
    if (!m_buffered || (op != "create" && op != "set" && op != "bulkset" ))
    {
        m_pipe->flush();
    }
}

void ProducerTable::del(const string &key, const string &op, const string &prefix,
                        const std::vector<KeyOpFieldsValuesTuple> &vkco)
{
    if (m_dumpFile.is_open())
    {
        if (!m_firstItem)
            m_dumpFile << "," << endl;
        else
            m_firstItem = false;

        json j;
        string json_key = getKeyName(key);
        j[json_key] = json::object();
        j["OP"] = op;
        m_dumpFile << j.dump(4);
    }

    enqueueDbChange(key, "{}", "D" + op, prefix, vkco);
    if (!m_buffered)
    {
        m_pipe->flush();
    }
}

void ProducerTable::flush()
{
    m_pipe->flush();
}

}
