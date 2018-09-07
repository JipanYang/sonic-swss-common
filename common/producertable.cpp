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
     */
    string luaEnque =
        "redis.call('LPUSH', KEYS[1], ARGV[1], ARGV[2], ARGV[3]);"
        "redis.call('PUBLISH', KEYS[2], ARGV[4]);";

    m_shaEnque = m_pipe->loadRedisScript(luaEnque);

    // TODO: combine all followings with luaEnque and prepare one LUA script
    string luaHset =
        "for i = 0, #KEYS do\n"
        "    redis.call('HSET', KEYS[1 + i], ARGV[1 + i * 2], ARGV[2 + i * 2])\n"
        "end\n";
    m_shaHset = m_pipe->loadRedisScript(luaHset);

    string luaHdel =
        "for i = 0, #KEYS do\n"
        "    redis.call('HDEL', KEYS[1 + i], ARGV[1 + i])\n"
        "end\n";
    m_shaHdel = m_pipe->loadRedisScript(luaHdel);

    string luaDel =
        "for i = 0, #KEYS do\n"
        "    redis.call('DEL', KEYS[1 + i])\n"
        "end\n";
    m_shaDel = m_pipe->loadRedisScript(luaDel);
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
    RedisCommand command;

    command.format(
        "EVALSHA %s 2 %s %s %s %s %s %s",
        m_shaEnque.c_str(),
        getKeyValueOpQueueTableName().c_str(),
        getChannelName().c_str(),
        key.c_str(),
        value.c_str(),
        op.c_str(),
        "G");

    if (vkco.size() == 0)
    {
        m_pipe->push(command, REDIS_REPLY_NIL);
    }
    else
    {
        vector<string> hsetKeys, hsetFV;
        vector<string> hdelKeys, hdelFields;
        vector<string> delKeys;
        for (const auto& kco: vkco)
        {
            if (kfvOp(kco) == "HMSET" || kfvOp(kco) == "HSET")
            {
                hsetKeys.insert(hsetKeys.end(), kfvFieldsValues(kco).size(), kfvKey(kco));

                for (const auto& iv: kfvFieldsValues(kco))
                {
                    hsetFV.emplace_back(fvField(iv));
                    hsetFV.emplace_back(fvValue(iv));
                }
            }
            else if (kfvOp(kco) == "HDEL")
            {
                hdelKeys.emplace_back(kfvKey(kco));
                hdelFields.emplace_back(fvField(kfvFieldsValues(kco)[0]));
            }
            else if (kfvOp(kco) == "DEL")
            {
                delKeys.emplace_back(kfvKey(kco));
            }
            else
            {
                SWSS_LOG_ERROR("Unsupported redis op type %s",
                    kfvOp(kco).c_str());
                throw system_error(make_error_code(errc::invalid_argument),
                    "Unsupported redis op type");
            }
        }

        if (hsetKeys.size() > 0)
        {
            vector<string> args;
            args.emplace_back("EVALSHA");
            args.emplace_back(m_shaHset);
            args.emplace_back(to_string(hsetKeys.size()));
            for (const auto& ik: hsetKeys)
            {
                args.emplace_back(ik);
            }
            for (const auto& ifv: hsetFV)
            {
                args.emplace_back(ifv);
            }
            transformAndPush(args);
        }

        if (hdelKeys.size() > 0)
        {
            vector<string> args;
            args.emplace_back("EVALSHA");
            args.emplace_back(m_shaHdel);
            args.emplace_back(to_string(hdelKeys.size()));
            for (const auto& ik: hdelKeys)
            {
                args.emplace_back(ik);
            }
            for (const auto& ifv: hdelFields)
            {
                args.emplace_back(ifv);
            }
            transformAndPush(args);
        }

        if (delKeys.size() > 0)
        {
            vector<string> args;
            args.emplace_back("EVALSHA");
            args.emplace_back(m_shaDel);
            args.emplace_back(to_string(delKeys.size()));
            for (const auto& ik: delKeys)
            {
                args.emplace_back(ik);
            }
            transformAndPush(args);
        }

        // Only try to flush with last command, eventually redis multi/exec transaction is needed
        m_pipe->push(command, REDIS_REPLY_NIL);
    }
}

void ProducerTable::transformAndPush(const vector<string> &args)
{
    // Transform data structure
    vector<const char *> args1;
    transform(args.begin(), args.end(), back_inserter(args1), [](const string &s) { return s.c_str(); } );

    // Invoke redis command
    RedisCommand cmd;
    cmd.formatArgv((int)args1.size(), &args1[0], NULL);
    m_pipe->push(cmd, REDIS_REPLY_NIL, false);
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
    // Only buffer continuous "set/set" or "del" operations
    if (!m_buffered || (op != "set" && op != "bulkset" ))
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
