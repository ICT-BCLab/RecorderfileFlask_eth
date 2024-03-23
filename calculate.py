# coding=utf-8
import pandas as pd
import numpy as np
from datetime import datetime
from decimal import Decimal
import time
import os


# 2.1交易池输入通量
def transaction_pool_input_throughput(input_path, output_path,
                                      check_column_name=True, add_column_name=False,
                                      test=False, batch_size=100000):
    start_time = time.time()
    try:
        data = pd.read_csv(os.path.join(input_path, "transaction_pool_input_throughput.csv"))
    except:
        raise Exception("transaction_pool_input_throughput.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "tx_id", "source"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("transaction_pool_input_throughput check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception(
                        "transaction_pool_input_throughput check column name fail! column:" + str(i) + " unmatched")
            print("transaction_pool_input_throughput check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 3:
                # 指标测量时间 交易ID 交易来源
                data.columns = ["measure_time", "tx_id", "source"]
            else:
                raise Exception("transaction_pool_input_throughput add_column_name fail! column count unmatched")
            print("transaction_pool_input_throughput add_column_name finish!")
        # 如果只计算部分数据
        if test:
            data = data[:batch_size]
        data.drop("tx_id", inplace=True, axis=1)

        data = data["measure_time"].str.slice(stop=19)  # 只保留需要用到的列并截取到20位保留到秒
        res = data.value_counts(sort=False)  # 统计每一秒的交易数量
        res = res.to_frame(name="transaction_pool_input_throughput")
        res.to_csv(os.path.join(output_path, "transaction_pool_input_throughput_result.csv"),
                   index_label="measure_time", index=True)
        print("calculate transaction_pool_input_throughput finish! time cost =", time.time() - start_time)


# 2.2平均传输延时 加了按秒合并 统一为毫秒
def net_p2p_transmission_latency(input_path, output_path,
                                 check_column_name=True, add_column_name=False,
                                 test=False, batch_size=3000):
    start_time = time.time()
    try:
        data = pd.read_csv(os.path.join(input_path, "net_p2p_transmission_latency.csv"))
    except:
        print("net_p2p_transmission_latency.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "peer_id", "peer1_deliver_time", "peer2_receive_time", "peer2_deliver_time",
                   "peer1_receive_time"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("net_p2p_transmission_latency check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception(
                        "net_p2p_transmission_latency check column name fail! column:" + str(i) + " unmatched")
            print("net_p2p_transmission_latency check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 7:
                data.columns = ["measure_time", "peer_id", "peer1_deliver_time", "peer2_receive_time",
                                "peer2_deliver_time", "peer1_receive_time"]
            else:
                raise Exception("net_p2p_transmission_latency add_column_name fail! column count unmatched")
            print("net_p2p_transmission_latency add_column_name finish!")
        # 如果只计算部分数据
        if test:
            data = data[:batch_size]
        # data = data.sort_values(["measure_time"])
        # print(data.head(10))
        measure_time = []
        transmission_latencies = []
        # 循环遍历每一行
        # for row in data.itertuples(): # 更快的迭代器
        # 版本1
        for data_row in data.itertuples():
            if (len(data_row[2]) == 7):
                continue
            time1 = data_row.peer1_deliver_time
            time2 = data_row.peer2_receive_time
            time3 = data_row.peer2_deliver_time
            time4 = data_row.peer1_receive_time
            t = data_row.measure_time

            t1 = datetime.strptime(time1, "%Y-%m-%d %H:%M:%S.%f")
            t2 = datetime.strptime(time2, "%Y-%m-%d %H:%M:%S.%f")
            t3 = datetime.strptime(time3, "%Y-%m-%d %H:%M:%S.%f")
            t4 = datetime.strptime(time4, "%Y-%m-%d %H:%M:%S.%f")

            # 微秒时间戳
            timestamp1 = int(t1.timestamp() * 1e6)
            timestamp2 = int(t2.timestamp() * 1e6)
            timestamp3 = int(t3.timestamp() * 1e6)
            timestamp4 = int(t4.timestamp() * 1e6)

            # 结果为毫秒类型
            transmission_latency = float(timestamp2 + timestamp4 - timestamp1 - timestamp3) / 2000
            # transmission_latency = t2 + t4 - t1 - t3
            transmission_latencies.append(transmission_latency)
            measure_time.append(t)

        res = pd.DataFrame()
        res["measure_time"] = measure_time
        res["net_p2p_transmission_latency"] = transmission_latencies
        res_time = res["measure_time"].str.slice(stop=19)
        res["measure_time"] = res_time
        res = res.groupby("measure_time").aggregate("mean")
        res.to_csv(os.path.join(output_path, "net_p2p_transmission_latency_result.csv"), index_label="measure_time",
                   index=True)
        print("calculate net_p2p_transmission_latency finish! time cost =", time.time() - start_time)


# 2.3宽带利用率 缺少节点带宽TotalBandwitch
# SUM(MessageSize)/测量时长/TotalBandwitch时长为1s，
def peer_message_throughput(input_path, output_path,
                            check_column_name=True, add_column_name=False,
                            test=False, batch_size=10000):
    start_time = time.time()
    try:
        data = pd.read_csv(os.path.join(input_path, "peer_message_throughput.csv"))
    except:
        print("peer_message_throughput.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "message_type", "message_size"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("peer_message_throughput check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("peer_message_throughput check column name fail! column:" + str(i) + " unmatched")
            print("peer_message_throughput check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 3:
                data.columns = ["measure_time", "message_type", "message_size"]
            else:
                raise Exception("peer_message_throughput add_column_name fail! column count unmatched")
            print("peer_message_throughput add_column_name finish!")
        if test:
            data = data[:batch_size]
        data = data.drop("message_type", axis=1)
        mtime = data["measure_time"].str.slice(stop=19)
        data["measure_time"] = mtime
        measure_time = []
        peer_message_throughput = []
        id = data.loc[0].measure_time
        count = 0
        for row in data.itertuples():
            if row.measure_time != id:
                measure_time.append(id)
                peer_message_throughput.append(count)
                id = row.measure_time
                count = row.message_size
            else:
                count += row.message_size
        measure_time.append(id)
        peer_message_throughput.append(count)
        res = pd.DataFrame()
        res["measure_time"] = measure_time
        res["peer_message_throughput"] = peer_message_throughput
        res.to_csv(os.path.join(output_path, "peer_message_throughput_result.csv"), index=False)
        print("calculate peer_message_throughput finish! time cost =", time.time() - start_time)


# 2.4状态数据写入吞吐量
def db_state_write_rate(input_path, output_path,
                        check_column_name=True, add_column_name=False,
                        test=False, batch_size=10000):
    start_time = time.time()
    try:
        data = pd.read_csv(os.path.join(input_path, "db_state_write_rate.csv"))
    except:
        print("db_state_write_rate.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "block_height", "block_hash", "write_duration"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("db_state_write_rate check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("db_state_write_rate check column name fail! column:" + str(i) + " unmatched")
            print("db_state_write_rate check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 5:
                data.columns = ["measure_time", "block_height", "block_hash", "write_duration"]
            else:
                raise Exception("db_state_write_rate add_column_name fail! column count unmatched")
            print("db_state_write_rate add_column_name finish!")
        if test:
            data = data[:batch_size]
        data.drop(["block_height", "block_hash"], axis=1, inplace=True)
        data.columns = ["measure_time", "db_state_write_rate"]

        data_time = data["measure_time"].str.slice(stop=19)
        data["measure_time"] = data_time

        res = pd.DataFrame()
        res["measure_time"] = data["measure_time"]
        res["db_state_write_rate"] = data["db_state_write_rate"]
        res.to_csv(os.path.join(output_path, "db_state_write_rate_result.csv"), index=False)
        print("calculate db_state_write_rate finish! time cost =", time.time() - start_time)


# 2.5状态数据读取吞吐量
def db_state_read_rate(input_path, output_path,
                       check_column_name=True, add_column_name=False,
                       test=False, batch_size=10000):
    start_time = time.time()
    try:
        data = pd.read_csv(os.path.join(input_path, "db_state_read_rate.csv"))
    except:
        print("db_state_read_rate.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "block_hash", "read_duration"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("db_state_read_rate check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("db_state_read_rate check column name fail! column:" + str(i) + " unmatched")
            print("db_state_read_rate check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 4:
                data.columns = ["measure_time", "block_hash", "read_duration"]
            else:
                raise Exception("db_state_read_rate add_column_name fail! column count unmatched")
            print("db_state_read_rate add_column_name finish!")
        if test:
            data = data[:batch_size]
        data.drop(["block_hash"], axis=1, inplace=True)
        data.columns = ["measure_time", "db_state_read_rate"]
        data_time = data["measure_time"].str.slice(stop=19)
        data["measure_time"] = data_time
        res = pd.DataFrame()
        res["measure_time"] = data["measure_time"]
        res["db_state_read_rate"] = data["db_state_read_rate"]
        res.to_csv(os.path.join(output_path, "db_state_read_rate_result.csv"), index=False)
        print("calculate db_state_read_rate finish! time cost =", time.time() - start_time)


def transform_time(t):
    res = 0
    try:
        res = time.mktime(time.strptime(str(t).strip("\t"), '%Y-%m-%d %H:%M:%S.%f')) + float(
            t[-6:]) / 1000000
        return res
    except:
        try:
            res = time.mktime(time.strptime(str(t).strip("\t"), '%Y-%m-%d %H:%M:%S'))
            return res
        except:
            raise Exception("time format wrong! time:" + str(t))


# 2.6交易排队时延？没有除以SUM(TxID)？还是说每个时间只有一个块？单位是m
def tx_queue_delay(input_path, output_path,
                   check_column_name=True, add_column_name=False,
                   test=False, batch_size=100000):
    start_time = time.time()
    try:
        data = pd.read_csv(os.path.join(input_path, "tx_queue_delay.csv"))
    except:
        print("tx_queue_delay.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "tx_hash", "in/outFlag"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("tx_queue_delay check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("tx_queue_delay check column name fail! column:" + str(i) + " unmatched")
            print("tx_queue_delay check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 3:
                data.columns = ["measure_time", "tx_hash", "in/outFlag"]
            else:
                raise Exception("tx_queue_delay add_column_name fail! column count unmatched")
            print("tx_queue_delay add_column_name finish!")
        if test:
            data = data[:batch_size]
        gb = data.groupby(by="tx_hash")
        measure_time_list = []
        tx_queue_delay_list = []
        for tx_id_qude, group_data in gb:
            if len(group_data) == 1:
                continue
            in_index = 0
            out_index = 1
            limit = len(group_data)
            while (in_index < limit and out_index < limit):
                if group_data.iloc[out_index, 2] != "out" or group_data.iloc[in_index, 2] != "in":
                    raise Exception("unmatched in and out!")
                out_time = transform_time(group_data.iloc[out_index, 0])
                in_time = transform_time(group_data.iloc[in_index, 0])
                measure_time_list.append(group_data.iloc[out_index, 0])
                tx_queue_delay_list.append((out_time - in_time) * 1000)
                in_index += 2
                out_index += 2
        res = pd.DataFrame()
        res["measure_time"] = measure_time_list

        # 如果为19，则会把毫秒相同的去掉
        res_time = res["measure_time"].str.slice(stop=26)
        res["measure_time"] = res_time
        res["tx_queue_delay"] = tx_queue_delay_list
        res = res.groupby("measure_time").aggregate("mean")
        res.sort_values("measure_time", inplace=True)
        save_path = os.path.join(output_path, "tx_queue_delay_result.csv")
        res.to_csv(save_path, encoding='utf-8', index=True)
        print("calculate tx_queue_delay finish! time cost =", time.time() - start_time)


def get_time(str_time):
    try:
        time = datetime.strptime(str_time, '%Y-%m-%d %H:%M:%S.%f')
        return time
    except:
        try:
            time = datetime.strptime(str_time, '%Y-%m-%d %H:%M:%S')
            return time
        except:
            raise Exception("time format error:" + str_time)


# 3.1出块时延 BlockConfirmTime - BlockGenTime 单位毫秒
# height匹配数据后统一转化为毫秒并相减
def block_commit_duration(input_path, output_path,
                          check_column_name=True, add_column_name=False,
                          test=False, batch_size=10000):
    start_time = time.time()
    try:
        df_starts = pd.read_csv(os.path.join(input_path, "block_commit_duration_start.csv"))
        df_ends = pd.read_csv(os.path.join(input_path, "block_commit_duration_end.csv"))
    except:
        print("block_commit_duration_start.csv or block_commit_duration_end.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "block_height"]
            now = []
            for name in df_starts.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("block_commit_duration check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("block_commit_duration check column name fail! column:" + str(i) + " unmatched")
            ans = ["measure_time", "block_height", "block_hash", "block_tx_count"]
            now = []
            for name in df_ends.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("block_commit_duration check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("block_commit_duration check column name fail! column:" + str(i) + " unmatched")
            print("block_commit_duration check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if df_starts.shape[1] == 3 and df_ends.shape[1] == 3:
                df_starts.columns = ["measure_time", "block_height"]
                df_ends.columns = ["measure_time", "block_height", "block_hash", "block_tx_count"]
            else:
                raise Exception("block_commit_duration add_column_name fail! column count unmatched")
            print("block_commit_duration add_column_name finish!")
        if test:
            df_starts = df_starts[:batch_size]
            df_ends = df_ends[:batch_size]
        df_starts.drop_duplicates(subset="block_height", keep="first", inplace=True)
        df_ends.drop(["block_hash", "block_tx_count"], axis=1, inplace=True)
        df_starts.rename(columns={"measure_time": "sendtime"}, inplace=True)
        df_ends.rename(columns={"measure_time": "confirmtime"}, inplace=True)
        df_combine = pd.merge(df_starts, df_ends, on="block_height")
        time_stamp_list = []
        block_delay_list = []
        for data in df_combine.itertuples():
            time_stamp_list.append(data.confirmtime)
            confirm_time = get_time(data.confirmtime)
            send_time = get_time(data.sendtime)
            block_delay = (confirm_time - send_time).total_seconds()
            block_delay_list.append(block_delay)
        res = pd.DataFrame()
        res["measure_time"] = time_stamp_list
        res['block_commit_duration'] = block_delay_list

        res_time = res["measure_time"].str.slice(stop=19)
        res["measure_time"] = res_time
        res = res.groupby("measure_time").aggregate("mean")
        res.to_csv(os.path.join(output_path, "block_commit_duration_result.csv"), index=True)
        print("calculate block_commit_duration finish! time cost =", time.time() - start_time)


# 3.2交易吞吐量 BlockTxNum / (BlockConfirmTime - BlockPackTime)
# packtime和gentime都用的sendtime？
def tx_in_block_tps(input_path, output_path, check_column_name=True, add_column_name=False, test=False,
                    batch_size=10000):
    start_time = time.time()
    try:
        df_starts = pd.read_csv(os.path.join(input_path, "block_commit_duration_start.csv"))
        df_ends = pd.read_csv(os.path.join(input_path, "block_commit_duration_end.csv"))
    except:
        print("block_commit_duration_start.csv or block_commit_duration_end.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "block_height"]
            now = []
            for name in df_starts.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("tx_tps check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("tx_tps check column name fail! column:" + str(i) + " unmatched")
            ans = ["measure_time", "block_height", "block_hash", "block_tx_count"]
            now = []
            for name in df_ends.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("tx_tps check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("tx_tps check column name fail! column:" + str(i) + " unmatched")
            print("tx_tps check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if df_starts.shape[1] == 3 and df_ends.shape[1] == 3:
                df_starts.columns = ["measure_time", "block_height"]
                df_ends.columns = ["measure_time", "block_height", "block_hash", "block_tx_count"]
            else:
                raise Exception("tx_tps add_column_name fail! column count unmatched")
            print("tx_tps add_column_name finish!")
        if test:
            df_starts = df_starts[:batch_size]
            df_ends = df_ends[:batch_size]
        df_starts.drop_duplicates(subset="block_height", keep="first", inplace=True)
        df_starts.rename(columns={"measure_time": "sendtime"}, inplace=True)
        df_ends.rename(columns={"measure_time": "confirmtime"}, inplace=True)
        df_combine = pd.merge(df_starts, df_ends, on="block_height")
        time_stamp_list = []
        tx_tps_list = []
        for data in df_combine.itertuples():
            time_stamp_list.append(data.confirmtime)
            confirm_time = get_time(data.confirmtime)
            send_time = get_time(data.sendtime)

            tx_tps = data.block_tx_count / (((confirm_time - send_time).total_seconds()) / 1000000)
            tx_tps = round(tx_tps, 2)
            tx_tps_list.append(tx_tps)
        res = pd.DataFrame()
        res["measure_time"] = time_stamp_list
        res['tx_tps'] = tx_tps_list

        res_time = res["measure_time"].str.slice(stop=19)
        res["measure_time"] = res_time
        res = res.groupby("measure_time").aggregate("mean")
        res.to_csv(os.path.join(output_path, "tx_in_block_tps_result.csv"), index=True)
        print("calculate tx_in_block_tps finish! time cost =", time.time() - start_time)


# 3.3区块验证效率 VerifyTime和VerifyNum
def block_validation_efficiency(input_path, output_path, check_column_name=True, add_column_name=False, test=False,
                                batch_size=10000):
    start_time = time.time()
    try:
        df_totals = pd.read_csv(os.path.join(input_path, "block_validation_efficiency_start.csv"))
        df_txs = pd.read_csv(os.path.join(input_path, "block_validation_efficiency_end.csv"))
    except:
        print("block_validation_efficiency_start.csv or block_validation_efficiency_end.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "block_hash", "block_validation_duration"]
            now = []
            for name in df_totals.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("block_validation_efficiency check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception(
                        "block_validation_efficiency check column name fail! column:" + str(i) + " unmatched")
            ans = ["measure_time", "block_hash", "block_tx_count"]
            now = []
            for name in df_txs.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("block_validation_efficiency check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception(
                        "block_validation_efficiency check column name fail! column:" + str(i) + " unmatched")
            print("block_validation_efficiency check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if df_totals.shape[1] == 2 and df_txs.shape[1] == 2:
                df_totals.columns = ["measure_time", "block_hash", "block_validation_duration"]
                df_txs.columns = ["measure_time", "block_hash", "block_tx_count"]
            else:
                raise Exception("block_validation_efficiency add_column_name fail! column count unmatched")
            print("block_validation_efficiency add_column_name finish!")
        if test:
            df_totals = df_totals[:batch_size]
            df_txs = df_txs[:batch_size]
        df_totals["measure_time"] = df_totals["measure_time"].str.slice(stop=19)
        df_txs.drop(["measure_time"], axis=1, inplace=True)
        df_combine = pd.merge(df_totals, df_txs, on="block_hash")
        res = pd.DataFrame()
        res["measure_time"] = df_combine["measure_time"]
        res["block_validation_duration"] = df_combine["block_validation_duration"]
        res["block_tx_count"] = df_combine["block_tx_count"]
        save_path = os.path.join(output_path, "block_validation_efficiency_result.csv")
        res.to_csv(save_path, index=False)
        print("calculate  block_validation_efficiency finish! time cost =", time.time() - start_time)
        # res1 = df_totals.groupby("measure_time").aggregate(np.sum)
        # res2 = df_txs.groupby("measure_time").aggregate(np.sum)
        # res = pd.merge(res1,res2,on="measure_time")
        # save_path = os.path.join(output_path, "block_validation_efficiency_result.csv")
        # res.to_csv(save_path, index=True)
        # print("calculate  block_validation_efficiency finish! time cost =", time.time()-start_time)


def get_delay(str_confirm, str_send):
    t_confirm = get_time(str_confirm)
    t_send = get_time(str_send)
    return (t_confirm - t_send).total_seconds()


# 3.4交易确认时延 单位是毫秒 TxConfirmaTime - TxSendTime
def tx_delay(input_path, output_path,
             check_column_name=True, add_column_name=False,
             test=False, batch_size=3000):
    start_time = time.time()
    try:
        df_starts = pd.read_csv(os.path.join(input_path, "tx_delay_start.csv"))
        df_ends = pd.read_csv(os.path.join(input_path, "tx_delay_end.csv"))
    except:
        print("tx_delay_starts.csv or tx_delay_ends.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "tx_hash"]
            now = []
            for name in df_starts.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("tx_confirm_delay check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("tx_confirm_delay check column name fail! column:" + str(i) + " unmatched")
            ans = ["measure_time", "block_height", "tx_hash"]
            now = []
            for name in df_ends.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("tx_confirm_delay check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("tx_confirm_delay check column name fail! column:" + str(i) + " unmatched")
            print("tx_confirm_delay check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if df_starts.shape[1] == 2 and df_ends.shape[1] == 2:
                df_starts.columns = ["measure_time", "tx_hash"]
                df_ends.columns = ["measure_time", "bloch_height", "tx_hash"]
            else:
                raise Exception("tx_queue_delay add_column_name fail! column count unmatched")
            print("tx_queue_delay add_column_name finish!")
        if test:
            df_starts = df_starts[:batch_size]
            df_ends = df_ends[:batch_size]
        df_starts.rename(columns={"measure_time": "sendtime"}, inplace=True)
        df_ends.rename(columns={"measure_time": "confirmtime"}, inplace=True)
        df_combine = pd.merge(df_starts, df_ends, on="tx_hash")
        confirm_delay = []
        for row in df_combine.itertuples():  # 更快的迭代器
            confirm_delay.append(get_delay(row.confirmtime, row.sendtime))
        res = pd.DataFrame()
        res["measure_time"] = df_combine["sendtime"]
        res['tx_confirm_delay'] = confirm_delay
        res = res.groupby("measure_time").aggregate("mean")
        res.sort_values("measure_time", inplace=True)
        res.to_csv(os.path.join(output_path, "tx_delay_result.csv"), index=True)
        print("calculate tx_delay finish! time cost =", time.time() - start_time)


# 3.7 3.8
def clique_round_time(input_path, output_path,
                      check_column_name=True, add_column_name=False):
    start_time = time.time()
    try:
        # 读取数据
        data = pd.read_csv(input_path + 'consensus_clique_cost.csv', na_values=" NaN")
    except:
        print("consensus_tbft_cost.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["block_height", "clique_start", "clique_end", "cost_time"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("clique_round_time check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("clique_round_time check column name fail! column:" + str(i) + " unmatched")
            print("clique_round_time check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 10:
                data.columns = ["block_height", "clique_start", "clique_end", "cost_time"]
            else:
                raise Exception("clique_round_time add_column_name fail! column count unmatched")
            print("clique_round_time add_column_name finish!")

        res = pd.DataFrame()
        res["measure_time"] = data["clique_end"]
        res["cost_time"] = data["cost_time"]

        # fixed_value = "2030-11-24 15:53:07"  # 设置固定的值
        # res["measure_time"] = [fixed_value] * len(res)

        res.to_csv(os.path.join(output_path, "consensus_clique_cost_result.csv"), index=False)
        print("calculate clique_round_time finish! time cost =", time.time() - start_time)
        # # 根据height和round来分组，其他字段计算返回平均值
        # df1 = data
        # #df1=origin_data.groupby(['height','round']).mean()
        # #print(df1.columns)
        # # 去掉不需要的列
        # df1.drop(columns = ['ID','HeightTotalTime'],inplace = True)
        # df1=df1.round({'Proposal': 2, 'Prevote': 2, 'Precommit': 2, 'Commit': 2, 'RoundTotalTime': 2})
        # df1["Proposal"] = df1["Proposal"].div(1000000)
        # df1["Prevote"] = df1["Prevote"].div(1000000)
        # df1["Precommit"] = df1["Precommit"].div(1000000)
        # df1["Commit"] = df1["Commit"].div(1000000)
        # df1["RoundTotalTime"] = df1["RoundTotalTime"].div(1000000)
        # # 取分组后每一组的第一列
        # df2=data.groupby(['Height','Round']).apply(lambda x: x.iloc[0])
        # # 取出时间戳
        # df3=df2['Tbfttimestamp']
        # res = pd.merge(df3, df1, on=['Height','Round'])
        # res = res.reset_index(drop = True)
        # res.drop(["Height","Round","Tbfttimestamp_x"],axis=1,inplace=True)
        # res.columns = ["measure_time","proposal","prevote","precommit","commit","round_total_time"]

        # res_time = res["measure_time"].str.slice(stop=19)
        # res["measure_time"] = res_time
        # res = res.groupby("measure_time").aggregate(np.mean)
        # res.to_csv(output_path+'tbft_result.csv', index=True)
        # print("calculate clique_round_time finish ! time cost =", time.time()-start_time)


# 4.2 合约执行时间
def contract_time(input_path, output_path,
                  check_column_name=True, add_column_name=False):
    start_time = time.time()
    try:
        # 读取数据
        data = pd.read_csv(input_path + 'contract_time.csv', na_values=" NaN")
    except:
        print("contract_time.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["TxHash", "ContractAddr", "StartTime", "EndTime", "ExecTime"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("clique_round_time check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("contract_time check column name fail! column:" + str(i) + " unmatched")
            print("contract_time check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 10:
                data.columns = ["TxHash", "ContractAddr", "StartTime", "EndTime", "ExecTime"]
            else:
                raise Exception("contract_time add_column_name fail! column count unmatched")
            print("contract_time add_column_name finish!")
        res = pd.DataFrame()
        res["measure_time"] = data["EndTime"]
        res["ExecTime"] = data["ExecTime"]
        # fixed_value = "2030-11-24 15:53:08"  # 设置固定的值
        # res["measure_time"] = [fixed_value] * len(res)
        res.to_csv(os.path.join(output_path, "contract_time_result.csv"), index=False)
        print("calculate contract_time finish! time cost =", time.time() - start_time)


# 4.1交易冲突率
def block_tx_conflict_rate(input_path, output_path,
                           check_column_name=True, add_column_name=False):
    start_time = time.time()
    try:
        data = pd.read_csv(os.path.join(input_path, "block_tx_conflict_rate.csv"))
    except:
        print("block_tx_conflict_rate.csv is not in the input path")
    else:
        # 如果检查数据来源的列名
        if check_column_name:
            ans = ["measure_time", "conflict_count", "block_height", "block_tx_count"]
            now = []
            for name in data.columns:
                now.append(name)
            if len(ans) != len(now):
                raise Exception("block_tx_conflict_rate check column name fail! unmatched column count")
            for i in range(len(ans)):
                if ans[i] != now[i]:
                    raise Exception("block_tx_conflict_rate check column name fail! column:" + str(i) + " unmatched")
            print("block_tx_conflict_rate check_column_name finish!")
        # 如果需要添加列名
        if add_column_name:
            if data.shape[1] == 4:
                data.columns = ["measure_time", "conflict_count", "block_height", "block_tx_count"]
            else:
                raise Exception("block_tx_conflict_rate add_column_name fail! column count unmatched")
            print("block_tx_conflict_rate add_column_name finish!")
        conflict_rate_list = []
        for row in data.itertuples():  # 更快的迭代器
            conflict_rate_list.append(row.conflict_count / row.block_tx_count)
        res = pd.DataFrame()
        res["measure_time"] = data["measure_time"]
        res["conflict_rate"] = conflict_rate_list

        res_time = res["measure_time"].str.slice(stop=19)
        res["measure_time"] = res_time
        res = res.groupby("measure_time").aggregate(np.mean)
        save_path = os.path.join(output_path, "block_tx_conflict_rate_result.csv")
        res.to_csv(save_path, index=False)
        print("calculate block_tx_conflict_rate finish ! time cost =", time.time() - start_time)


def preprocess(df):
    for index in range(df.shape[0]):  # 截取时间只保留到秒
        df.loc[index, "measure_time"] = df.loc[index, "measure_time"][:19]
    return df.groupby("measure_time").aggregate(np.mean)  # 秒作为label取平均，得到新的数据


def merge(name, old, preprocess_flag=False, sort=False):
    tmp = pd.DataFrame(pd.read_csv(os.path.join(output_path, name)))
    if preprocess_flag:
        t = time.time()
        tmp = preprocess(tmp)
        print("preprocess done time=", t - time.time())
    return pd.merge(old, tmp, on="measure_time", how="outer", sort=sort)


if __name__ == "__main__":
    input_path = "/Users/bethestar/Downloads/ethlog/mylog/"
    output_path = "/Users/bethestar/Downloads/ethlog/mylog/res/"
    # # 2.1 交易池输入通量
    # transaction_pool_input_throughput(input_path, output_path)
    # # 2.2 P2P网络平均传输时延
    # net_p2p_transmission_latency(input_path, output_path)
    # # 2.3 节点收发消息总量
    # peer_message_throughput(input_path, output_path)
    # 2.4 状态数据写入吞吐量
    db_state_write_rate(input_path, output_path)
    # 2.5 状态数据读取吞吐量
    db_state_read_rate(input_path, output_path)
    # 2.6交易排队时延
    tx_queue_delay(input_path, output_path)
    # 3.1出块时延
    block_commit_duration(input_path, output_path)
    # 3.2交易吞吐量
    tx_in_block_tps(input_path, output_path)
    # 3.3区块验证效率
    block_validation_efficiency(input_path, output_path)
    # 3.4交易确认时延
    tx_delay(input_path, output_path)
    # 3.7 3.8 一轮clique耗时
    clique_round_time(input_path, output_path)
    # 4.1 交易冲突率
    # block_tx_conflict_rate(input_path, output_path)
    # 4.2 合约执行时间
    contract_time(input_path, output_path)

    # data2_1 = pd.read_csv(os.path.join(output_path, "transaction_pool_input_throughput_result.csv"),skiprows=lambda x: x > 0 and 'measure_time' in x)
    # data2_2 = pd.read_csv(os.path.join(output_path, "net_p2p_transmission_latency_result.csv"))
    # res = pd.merge(data2_1, data2_2, on="measure_time", how="outer")
    # res = merge("peer_message_throughput_result.csv", res)  # 2.3
    # print("2.3 finish")
    # res = merge("db_state_write_rate_result.csv", res)  # 2.4
    res = pd.read_csv(os.path.join(output_path, "db_state_write_rate_result.csv"))
    print("2.4 finish")
    res = merge("db_state_read_rate_result.csv", res)  # 2.5
    print("2.5 finish")
    res = merge("tx_queue_delay_result.csv", res)  # 2.6
    print("2.6 finish")
    res = merge("block_commit_duration_result.csv", res)
    print("3.1 finish")
    res = merge("tx_in_block_tps_result.csv", res)
    print("3.2 finish")
    res = merge("block_validation_efficiency_result.csv", res)  # 3.3
    print("3.3 finish")
    res = merge("tx_delay_result.csv", res)  # 3.4
    print("3.4 finish")
    res = merge("consensus_clique_cost_result.csv", res)  # 3.7 3.8
    print("3.7 finish")
    res = merge("contract_time_result.csv", res)  # 4.1
    print("4.2 finish")
    # res = merge("block_tx_conflict_rate_result.csv", res) #4.2
    # print("4.1 finish")
    for index in range(res.shape[0]):  # 截取时间只保留到秒
        res.loc[index, "measure_time"] = "-".join(res.loc[index, "measure_time"].split(":"))
    res.columns = ["measure_time", "2.1交易池输入通量", "2.2P2P网络平均传输时延", "2.3节点收发消息总量",
                   "2.4数据库写入速率", "2.5数据库读取速率",
                   "2.6交易排队时延", "3.1出块耗时", "3.2块内交易吞吐量", "3.3区块验证效率-验证耗时",
                   "3.3区块验证效率-验证耗时", "3.4交易时延", "3.7每轮clique耗时", "4.2合约执行时间"]

    res.to_csv(output_path + "res.csv", index=True, encoding="utf_8_sig")