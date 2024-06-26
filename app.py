import json

import pandas as pd
import requests
import yaml
from flask import Flask, render_template, request, jsonify
from flask_paginate import Pagination, get_page_parameter

from pyecharts import options as opts
from pyecharts.charts import Bar, Line, Grid, Pie, Tab
from pyecharts.commons.utils import JsCode
from bs4 import BeautifulSoup
from datetime import datetime
from flask_cors import CORS
from web3 import Web3, HTTPProvider


client = Web3(HTTPProvider("http://localhost:8546"))


app = Flask(__name__, template_folder='templates', static_folder='resource', static_url_path="/")
CORS(app, supports_credentials=True)

filepath = "/Users/bethestar/Downloads/ethlog/mylog"
config_server = "127.0.0.1:9527"

# 把pyecharts自动生成的完整html文件转化成能嵌入到展示小模块的代码
def parse_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    chart_id = soup.find("div", {"class": "chart-container"}).get("id")
    chart_js = soup.find_all("script")[1]
    chart = "<div class=\"panel-draw d-flex flex-column justify-content-center align-items-center\" id=\"" + chart_id + "\"></div>"
    return chart, chart_js


# 【针对有多个tab的情况】把pyecharts自动生成的完整html文件转化成能嵌入到展示小模块的代码
def parse_html_tab(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    chart = soup.body
    return chart, ""


# 截短过长的哈希值
def shorten_id(node_id):
    return "0x" + node_id[:8] + "..."


# 读取csv文件，去掉重复表头行
def read_csv_without_duplicates(path):
    # 暂存表头
    header = pd.read_csv(path, nrows=0).columns.tolist()
    # 读取CSV文件，忽略表头
    df = pd.read_csv(path, skiprows=[0])
    # 删除所有重复行
    df = df.drop_duplicates(keep=False)
    # 将原始表头添加回DataFrame
    df.columns = header
    return df

# 根据格式化好的时间计算duration（返回的时间不带's'结尾）
def calculate_duration(end_time, start_time):
    formatted_time = "%Y-%m-%d %H:%M:%S.%f"
    end_time = datetime.strptime(end_time, formatted_time)
    start_time = datetime.strptime(start_time, formatted_time)
    return (end_time - start_time).total_seconds()

# 将Sub()函数获取的duration进行统一为s处理
def convert_duration_to_seconds(duration_str):
    # 定义单位转换因子
    conversion_factors = {
        'ms': 1e-3,
        'µs': 1e-6,
        'ns': 1e-9,
        's': 1,
        'm': 60,
        'h': 3600
    }
    # 单位
    for unit, factor in conversion_factors.items():
        if duration_str.endswith(unit):
            duration_str = duration_str.rstrip(unit)
            break
    # 将字符串转换为浮点数
    try:
        duration_seconds = float(duration_str) * factor
    except ValueError:
        return float('nan')
    return duration_seconds

# 统计分段并生成CDF图
def construct_cdf_chart(df, num_bins, title_name):
    # 将数据转换为float类型
    df = df.astype(float)
    # 移除负数和NaN值
    df = df[df >= 0].dropna()
    # 对数据进行分段
    bin_edges = pd.cut(df, bins=num_bins, include_lowest=True)
    # 统计每个分段的数量
    value_counts = bin_edges.value_counts(sort=False)
    # 计算累积频数
    cdf = value_counts.sort_index().cumsum()
    # 计算累积频数的百分比
    cdf_percent = cdf / cdf.iloc[-1] * 100

    # 保证从原点开始绘制CDF
    # 获取数据的最小值
    min_value = df.min()
    # 创建一个从0到最小值的区间，并将其累积分布百分比设为0
    cdf_percent = pd.concat([pd.Series([0], index=[pd.Interval(0, min_value, closed='left')]), cdf_percent])

    # 创建CDF图
    cdf_chart = (
        Line()
        .add_xaxis(cdf_percent.index.astype(str).tolist())
        .add_yaxis(series_name="累积分布",
                   y_axis=cdf_percent.tolist(),
                   areastyle_opts=opts.AreaStyleOpts(opacity=1, color="#173c8550"),
                   # label_opts=opts.LabelOpts(formatter="{@[1]}%",position="bottom")  # 格式化标签并放在数据点下方

                   label_opts=opts.LabelOpts(formatter=JsCode(
                            """
                            function (params) {
                                console.log(params);
                                return (params.value[1] * 1).toFixed(3) + '%';
                            }
                            """
                        ), position="right")
                   )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title_name + "累积分布函数"),
            toolbox_opts=opts.ToolboxOpts(),
            datazoom_opts=[opts.DataZoomOpts(range_start=0, range_end=100)],
            xaxis_opts=opts.AxisOpts(name=title_name + "区间"),
            yaxis_opts=opts.AxisOpts(name="累积分布百分比"),
            tooltip_opts=opts.TooltipOpts(),
        )
    )
    return cdf_chart



@app.route('/')
def index():
    # ---区块信息汇总---
    df_start_commit = read_csv_without_duplicates(filepath + '/block_commit_duration_start.csv')
    df_end_commit = read_csv_without_duplicates(filepath + '/block_commit_duration_end.csv')

    df_start_commit = df_start_commit.rename(columns={'measure_time': '打包时刻'})
    df_end_commit = df_end_commit.rename(columns={'measure_time': '落库时刻'})

    df_commit = pd.merge(df_start_commit, df_end_commit, on='block_height')
    df_commit.drop_duplicates(subset='block_height', keep='last', inplace=True)

    df_commit.loc[:, 'block_hash'] = df_commit.loc[:, 'block_hash'].apply(lambda x: shorten_id(x))

    df_valid = read_csv_without_duplicates(filepath + '/block_validation_efficiency.csv')
    df = pd.merge(df_valid, df_commit, on='block_height')

    df = df.dropna()

    df.rename(columns={'start_time': '开始验证时刻'}, inplace=True)
    df.rename(columns={'end_time': '结束验证时刻'}, inplace=True)
    df.rename(columns={'block_height': '块高'}, inplace=True)
    df.rename(columns={'block_hash': '区块哈希'}, inplace=True)
    df.rename(columns={'block_tx_count_x': '交易数量'}, inplace=True)

    df = df[['块高', '区块哈希', '交易数量', '打包时刻', '开始验证时刻', '结束验证时刻', '落库时刻']]
    df = df.reindex(columns=['块高', '区块哈希', '交易数量', '打包时刻', '开始验证时刻', '结束验证时刻', '落库时刻'])

    # ---交易信息汇总---
    df_tx_queue = read_csv_without_duplicates(filepath + '/tx_queue_delay.csv')
    df_in = df_tx_queue.loc[df_tx_queue['in/outFlag'] == 'in']
    df_out = df_tx_queue.loc[df_tx_queue['in/outFlag'] == 'out']

    df_in = df_in.rename(columns={'measure_time': '进入交易池时刻'})
    df_out = df_out.rename(columns={'measure_time': '离开交易池时刻'})
    df_tx_queue = pd.merge(df_in, df_out, on='tx_hash')

    df_tx_block = read_csv_without_duplicates(filepath + '/tx_delay_end.csv')[['tx_hash', 'block_height']]
    df_tx = pd.merge(df_tx_queue, df_tx_block, on='tx_hash')

    df_tx.loc[:, 'tx_hash'] = df_tx.loc[:, 'tx_hash'].apply(lambda x: shorten_id(x))

    df_tx.rename(columns={'block_height': '落库块高'}, inplace=True)
    df_tx.rename(columns={'tx_hash': '交易哈希'}, inplace=True)

    df_tx = df_tx[['交易哈希', '进入交易池时刻', '离开交易池时刻', '落库块高']]
    df_tx = df_tx.reindex(columns=['交易哈希', '进入交易池时刻', '离开交易池时刻', '落库块高'])

    # ---表格处理---
    # 获取当前页码
    page = request.args.get(get_page_parameter(), type=int, default=1)
    # 每页显示的数据量
    per_page = 5
    # 获取当前选中的选项卡
    active_tab = request.args.get('tab', 'tab1')
    # 根据选中的选项卡切换数据帧
    if active_tab == 'tab1':
        data = df
    elif active_tab == 'tab2':
        data = df_tx
    # 分页处理
    pagination = Pagination(page=page, per_page=per_page, total=data.shape[0], css_framework='bootstrap4')

    return render_template('board.html', data=data[(page - 1) * per_page:page * per_page], pagination=pagination,
                           active_tab=active_tab)


# 获取最新区块信息
@app.route('/get_latest_block', methods=['POST','GET'])
def get_latest_block():
    block_info = client.eth.get_block('latest')
    return jsonify(block_info)

# 获取节点数量
@app.route('/get_peer_cnt', methods=['POST','GET'])
def get_peer_cnt():
    return str(len(client.geth.admin.peers()))

# 获取区块数量
@app.route('/get_block_number', methods=['POST','GET'])
def get_block_number():
    return str(client.eth.block_number)

# 获取交易数量
@app.route('/get_tx_cnt', methods=['POST','GET'])
def get_tx_cnt():
    total = 0
    for i in range(0,client.eth.block_number+1):
        block = client.eth.get_block(i, full_transactions=True)
        total += len(block.transactions)

# 获取交易池tps
@app.route('/get_txpool_tps', methods=['POST','GET'])
def get_txpool_tps():
    # 读取csv文件
    df = pd.read_csv(filepath + "/transaction_pool_input_throughput.csv")
    if len(df) <= 0:
        return str(0)

    # 确定起始和结束索引
    if len(df) >= 500:
        start_index = -500
    elif len(df) >= 200:
        start_index = -200
    elif len(df) >= 100:
        start_index = -100
    elif len(df) >= 50:
        start_index = -50
    elif len(df) >= 10:
        start_index = -10
    elif len(df) >= 5:
        start_index = -5
    elif len(df) >= 2:
        start_index = -2
    else:
        return str(0)

    # 获取最后几条数据
    last_10_df = df.iloc[start_index:]
    sum_txs = last_10_df.shape[0]  # 最后几条记录的交易数
    start_time = str(last_10_df.iloc[0]['measure_time'])  # 最后几条记录中第一条的时间
    end_time = str(df.iloc[-1]['measure_time'])  # 最后一条记录的时间
    duration = calculate_duration(end_time, start_time)  # 最后几条记录的总记录时间

    # 计算TPS
    if duration > 0:
        tps = sum_txs / duration
    else:
        tps = 0

    return "%.2f" % tps

# 修改记录文件夹路径
@app.route('/changeFilepath', methods=["POST"])
def change_filepath():
    input_path = request.get_data()
    global filepath
    filepath = input_path.decode('utf-8')
    print("new_path", filepath)
    return "success"

# 修改config_server地址
@app.route('/changeConfigServer', methods=["POST"])
def change_config_server():
    input_path = request.get_data()
    global config_server
    config_server = input_path.decode('utf-8')
    print("new_config_server", config_server)
    return "success"

# 更新开关状态
@app.route('/changeSwitch', methods=["POST", "GET", "PUT"])
def change_switch():
    global config_server
    # 如果info["server"]不为空，才更新config_server
    info = request.get_json()
    if info["server"]!="":
        config_server = info["server"]
    url = 'http://' + config_server + '/config/accessconfig'
    data = info["new_yaml"]
    headers = {'Content-Type': 'application/x-yaml'}
    response = requests.put(url, data=data, headers=headers)
    if response.status_code == 200:
        return jsonify({'status': 'success', 'data': yaml.safe_load(response.text)})
    else:
        return jsonify({'status': 'error'})


# 获取最新的开关状态
@app.route('/updateSwitch', methods=["POST", "GET", "PUT"])
def update_switch():
    url = 'http://' + config_server + '/config/accessconfig'
    response = requests.get(url)
    if response.status_code == 200:
        return jsonify({'status': 'success', 'data': yaml.safe_load(response.text)})
    else:
        return jsonify({'status': 'error'})


# --网络层--
# 节点收发消息总量
@app.route("/PeerMessageThroughput")
def get_peer_message_throughput():
    filename = "/peer_message_throughput.csv"
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + filename)
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    # 获取message_size中的最大值最小值
    min_value = float(min(df["message_size"].tolist()))
    max_value = float(max(df["message_size"].tolist()))

    # 按照message_type拆分数据
    received_df = df.loc[df['message_type'] == 'Received']
    sent_df = df.loc[df['message_type'] == 'Sent']

    line1 = (
        Line()
        .add_xaxis(received_df['measure_time'].tolist())
        .add_yaxis(series_name="接收消息大小", y_axis=received_df["message_size"].tolist(), is_smooth=True)
        .set_global_opts(title_opts=opts.TitleOpts(title="节点收发消息总量-接收"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=0, range_end=5, xaxis_index=0),
                             opts.DataZoomOpts(range_start=10, range_end=15, xaxis_index=1),
                         ],
                         xaxis_opts=opts.AxisOpts(name="测量时刻"),
                         yaxis_opts=opts.AxisOpts(name="消息大小"),
                         legend_opts=opts.LegendOpts(pos_left="center"),
                         )
    )

    line2 = (
        Line()
        .add_xaxis(sent_df['measure_time'].tolist())
        .add_yaxis(series_name="发送消息大小", y_axis=sent_df["message_size"].tolist(), is_smooth=True)
        .set_global_opts(title_opts=opts.TitleOpts(title="节点收发消息总量-发送", pos_top="50%"),
                         xaxis_opts=opts.AxisOpts(name="测量时刻"),
                         yaxis_opts=opts.AxisOpts(name="消息大小"),
                         legend_opts=opts.LegendOpts(pos_left="center", pos_top="50%"),
                         )
    )

    grid = (
        Grid()
        .add(chart=line1, grid_opts=opts.GridOpts(pos_bottom="60%"))
        .add(chart=line2, grid_opts=opts.GridOpts(pos_top="60%"))
    )

    chart, chart_js = parse_html(grid.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# P2P网络平均传输时延
@app.route("/NetP2PTransmissionLatency")
def get_net_p2p_transmission_latency():
    filename = "/net_p2p_transmission_latency.csv"
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + filename)
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")
    # 取出send_id列的唯一值，后续作为标题
    send_id = df['peer_id'].unique()[0]
    # 计算时间差
    df['p1top2'] = df.apply(lambda row: calculate_duration(row['peer2_receive_time'], row['peer1_deliver_time']), axis=1)
    df['p2top1'] = df.apply(lambda row: calculate_duration(row['peer1_receive_time'], row['peer2_deliver_time']), axis=1)
    df['duration'] = (df['p1top2'] + df['p2top1']) / 2
    df = df[df['duration'] >= 0]

    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(df) != 0:
        min_value = float(min(df["duration"].tolist()))
        max_value = float(max(df["duration"].tolist()))
    bar = (
        Bar()
        .add_xaxis(df["measure_time"].tolist())
        .add_yaxis(series_name="平均传输时间", y_axis=df["duration"].tolist())
        .set_global_opts(title_opts=opts.TitleOpts(title="从" + shorten_id(send_id) + "发出消息计算结果"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=0, range_end=100),
                         ],
                         xaxis_opts=opts.AxisOpts(name="测量时间"),
                         yaxis_opts=opts.AxisOpts(name="平均传输时间/s"),
                         legend_opts=opts.LegendOpts(pos_left="center"),
                         )

    )

    bar_cdf_hist = construct_cdf_chart(df['duration'], 10, "P2P平均传播时延")

    tab = Tab()
    tab.add(bar, "按时刻查看")
    tab.add(bar_cdf_hist, "按累计分布查看")
    chart, chart_js = parse_html_tab(tab.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# --数据层--
# 数据库写入速率
@app.route("/DBStateWriteRate")
def get_db_state_write_rate():
    filename = "/db_state_write_rate.csv"
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + filename)
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    # 使用loc选择特定列
    new_df = df.loc[:, ['block_height', 'block_hash', 'write_duration']]
    # 应用转换函数到'write_duration'列
    new_df['write_duration'] = new_df['write_duration'].apply(convert_duration_to_seconds)
    # 重命名'write_duration'列为'value'
    new_df.rename(columns={'write_duration': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = new_df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    # 拼接标注字符串
    JsStr = "'数据库平均写入速率：" + str(new_df['value'].astype(float).mean()) + "秒/块'"
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(new_df['block_height'].tolist())
        .add_yaxis(series_name="写入耗时", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="数据库写入速率"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=40, range_end=60),
                         ],
                         xaxis_opts=opts.AxisOpts(name="区块高度"),
                         yaxis_opts=opts.AxisOpts(name="写入耗时/s"),
                         legend_opts=opts.LegendOpts(pos_left="center"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '区块哈希:'+ params.data.block_hash+ '</br>' +
                                            '写入耗时:'+ params.data.value+ 's' ;
                                 }
                                 """
                             )
                         ),
                         graphic_opts=[
                             opts.GraphicGroup(
                                 graphic_item=opts.GraphicItem(right="20%", top="15%"),
                                 children=[
                                     opts.GraphicRect(
                                         graphic_item=opts.GraphicItem(
                                             z=100, left="center", top="middle"
                                         ),
                                         graphic_shape_opts=opts.GraphicShapeOpts(width=320, height=30),
                                         graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(
                                             fill="#0b3a8a30",
                                             shadow_blur=8,
                                             shadow_offset_x=3,
                                             shadow_offset_y=3,
                                             shadow_color="rgba(0,0,0,0.3)",
                                         ),
                                     ),
                                     opts.GraphicText(
                                         graphic_item=opts.GraphicItem(
                                             left="center", top="middle", z=100
                                         ),
                                         graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                                             text=JsCode(
                                                 JsStr
                                             ),
                                             font="12px Microsoft YaHei",
                                             graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(
                                                 fill="#333"
                                             ),
                                         ),
                                     ),
                                 ],
                             )
                         ],
                         )
    )

    chart, chart_js = parse_html(bar.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# 数据库读取速率
@app.route("/DBStateReadRate")
def get_db_state_read_rate():
    filename = "/db_state_read_rate.csv"
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + filename)
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    # 使用loc选择特定列
    new_df = df.loc[:, ['block_hash', 'read_duration']]
    # 应用转换函数到'read_duration'列
    new_df['read_duration'] = new_df['read_duration'].apply(convert_duration_to_seconds)
    # 重命名'read_duration'列为'value'
    new_df.rename(columns={'read_duration': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = new_df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(df["measure_time"].tolist())
        .add_yaxis(series_name="读取耗时", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="数据库读取速率"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=45, range_end=55),
                         ],
                         xaxis_opts=opts.AxisOpts(name="开始读取时刻"),
                         yaxis_opts=opts.AxisOpts(name="读取耗时/s"),
                         legend_opts=opts.LegendOpts(pos_left="center"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '区块哈希:'+ params.data.block_hash + '</br>' +
                                            '读取耗时:'+ params.data.value+ 's';
                                 }
                                 """
                             )
                         ),
                         )

    )

    bar_hist = construct_cdf_chart(new_df['value'], 3, "数据库读取耗时")

    tab = Tab()
    tab.add(bar, "按时刻查看")
    tab.add(bar_hist, "按累计分布查看")
    chart, chart_js = parse_html_tab(tab.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# --共识层--
# 每轮Clique共识耗时
@app.route("/ConsensusCliqueCost")
def get_consensus_clique_cost():
    filename = "/consensus_clique_cost.csv"
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + filename)
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    # 使用loc选择特定列
    new_df = df.loc[:, ['block_height', 'cost_time']]
    # 应用转换函数到'cost_time'列
    new_df['cost_time'] = new_df['cost_time'].apply(convert_duration_to_seconds)
    # 将负筛掉
    new_df['cost_time'] = new_df['cost_time'].clip(lower=0)
    # 重命名'cost_time'列为'value'
    new_df.rename(columns={'cost_time': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = new_df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(new_df['block_height'].tolist())
        .add_yaxis(series_name="共识耗时", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="每轮Clique共识耗时"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=40, range_end=60),
                         ],
                         xaxis_opts=opts.AxisOpts(name="区块高度"),
                         yaxis_opts=opts.AxisOpts(name="共识耗时/s"),
                         legend_opts=opts.LegendOpts(pos_left="center"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '区块高度:'+ params.data.block_height+ '</br>' +
                                            '共识类型:'+ 'Clique' + '</br>' +
                                            '共识耗时:'+ params.data.value+ 's' ;
                                 }
                                 """
                             )
                         ),
                         )
    )

    chart, chart_js = parse_html(bar.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# --合约层--
# 合约执行时间
@app.route("/ContractTime")
def get_contract_time():
    filename = "/contract_time.csv"
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + filename)
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    # 使用loc选择特定列
    new_df = df.loc[:, ['tx_hash', 'contract_addr', 'exec_time']]
    # 应用转换函数到'exec_time'列
    new_df['exec_time'] = new_df['exec_time'].apply(convert_duration_to_seconds)
    # 重命名'exec_time'列为'value'
    new_df.rename(columns={'exec_time': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = new_df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(df["start_time"].tolist())
        .add_yaxis(series_name="执行时间", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="合约执行时间"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=45, range_end=55),
                         ],
                         xaxis_opts=opts.AxisOpts(name="开始执行时刻"),
                         yaxis_opts=opts.AxisOpts(name="执行时间/s"),
                         legend_opts=opts.LegendOpts(pos_left="center"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '合约地址:'+ params.data.contract_addr + '</br>' +
                                             '交易哈希:'+ params.data.tx_hash  + '</br>' +
                                             '执行时间:'+ params.data.value+ 's' ;
                                 }
                                 """
                             )
                         ),
                         )

    )

    bar_hist = construct_cdf_chart(new_df['value'], 5, "合约执行时间")

    tab = Tab()
    tab.add(bar, "按时刻查看")
    tab.add(bar_hist, "按累计分布查看")
    chart, chart_js = parse_html_tab(tab.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# --交易生命周期--
# 交易延迟
@app.route("/TxDelay")
def get_tx_delay():
    # 读取csv文件
    df_start = read_csv_without_duplicates(filepath + '/tx_delay_start.csv')
    df_end = read_csv_without_duplicates(filepath + '/tx_delay_end.csv')
    if len(df_start) <= 0 or len(df_end) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")
    # 重命名列
    df_start = df_start.rename(columns={'measure_time': 'start_time'})
    df_end = df_end.rename(columns={'measure_time': 'end_time'})
    # 合并df_start和df_end
    df = pd.merge(df_start, df_end, on='tx_hash')
    # 计算时间差
    df['duration'] = df.apply(lambda row: calculate_duration(row['end_time'], row['start_time']), axis=1)
    df = df[df['duration'] >= 0]

    # 重命名'duration'列为'value'
    df.rename(columns={'duration': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(df["start_time"].tolist())
        .add_yaxis(series_name="交易延迟", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="交易延迟"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=45, range_end=55),
                         ],
                         xaxis_opts=opts.AxisOpts(name="进入交易池时刻"),
                         yaxis_opts=opts.AxisOpts(name="交易延迟/s"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '区块高度:'+ params.data.block_height + '</br>' +
                                             '交易哈希:'+ params.data.tx_hash  + '</br>' +
                                             '交易延迟:'+ params.data.value+ 's' ;
                                 }
                                 """
                             )
                         ),
                         )

    )
    bar_cdf_hist = construct_cdf_chart(df['value'], 10, "交易延迟")

    tab = Tab()
    tab.add(bar, "按时刻查看")
    tab.add(bar_cdf_hist, "按累计分布查看")
    chart, chart_js = parse_html_tab(tab.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# 交易排队时延
@app.route("/TxQueueDelay")
def get_tx_queue_delay():
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + '/tx_queue_delay.csv')
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")
    # 根据"in/outFlag"列的值选择行，并分别赋值给df_in和df_out
    df_in = df.loc[df['in/outFlag'] == 'in']
    df_out = df.loc[df['in/outFlag'] == 'out']
    # 重命名列
    df_in = df_in.rename(columns={'measure_time': 'start_time'})
    df_out = df_out.rename(columns={'measure_time': 'end_time'})
    # 合并df_in和df_out
    df = pd.merge(df_in, df_out, on='tx_hash')
    # 计算时间差
    df['duration'] = df.apply(lambda row: calculate_duration(row['end_time'], row['start_time']), axis=1)
    df = df[df['duration'] >= 0]

    # 重命名'duration'列为'value'
    df.rename(columns={'duration': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(df["start_time"].tolist())
        .add_yaxis(series_name="交易排队时延", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="交易排队时延"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=45, range_end=55),
                         ],
                         xaxis_opts=opts.AxisOpts(name="进入交易池时刻"),
                         yaxis_opts=opts.AxisOpts(name="交易排队时延/s"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return  '交易哈希:'+ params.data.tx_hash  + '</br>' +
                                             '交易排队时延:'+ params.data.value+ 's' ;
                                 }
                                 """
                             )
                         ),
                         )

    )
    bar_hist = construct_cdf_chart(df['value'], 10, "交易排队时延")

    tab = Tab()
    tab.add(bar, "按时刻查看")
    tab.add(bar_hist, "按累计分布查看")
    chart, chart_js = parse_html_tab(tab.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# 交易池输入通量
@app.route("/TransactionPoolInputThroughput")
def get_transaction_pool_input_throughput():
    filename = "/transaction_pool_input_throughput.csv"
    # 读取csv文件
    df = read_csv_without_duplicates(filepath + filename)
    if len(df) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    sum_txs = df.shape[0]  # 当前记录的交易数
    start_time = str(df.iloc[0]['measure_time'])  # 第一条的时间
    end_time = str(df.iloc[-1]['measure_time'])  # 最后一条的时间
    duration = calculate_duration(end_time, start_time)  # 总记录时间
    txpool_input_throughput = sum_txs / duration  # 交易池输入通量

    # 把1修改为local 2修改为rpc
    df['source'] = df['source'].replace({1: 'local', 2: 'rpc'})

    # 拼接标注字符串
    JsStr = "['开始时间: " + start_time + "','结束时间: " + end_time + "','总记录时间: " + str(
        duration) + "s" "','交易数: " + str(sum_txs) + "','交易池输入通量: " + str(
        txpool_input_throughput) + "'].join('\\n')"
    type_counts = df["source"].value_counts()
    pie = (
        Pie()
        .add(
            series_name="交易来源",
            data_pair=[list(i) for i in type_counts.items()],  # 将Series转换为列表
        )
        .set_global_opts(graphic_opts=[
            opts.GraphicGroup(
                graphic_item=opts.GraphicItem(left="1%", top="15%"),
                children=[
                    opts.GraphicRect(
                        graphic_item=opts.GraphicItem(
                            z=100, left="center", top="middle"
                        ),
                        graphic_shape_opts=opts.GraphicShapeOpts(width=260, height=90),
                        graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(
                            fill="#0b3a8a30",
                            shadow_blur=8,
                            shadow_offset_x=3,
                            shadow_offset_y=3,
                            shadow_color="rgba(0,0,0,0.3)",
                        ),
                    ),
                    opts.GraphicText(
                        graphic_item=opts.GraphicItem(
                            left="center", top="middle", z=100
                        ),
                        graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                            text=JsCode(
                                JsStr
                            ),
                            font="14px Microsoft YaHei",
                            graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(
                                fill="#333"
                            ),
                        ),
                    ),
                ],
            )
        ], )
        .set_series_opts(
            tooltip_opts=opts.TooltipOpts(
                trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"
            ),
        )
    )

    chart, chart_js = parse_html(pie.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# 出块耗时
@app.route("/BlockCommitDuration")
def get_block_commit_duration():
    # 读取csv文件
    df_start = read_csv_without_duplicates(filepath + '/block_commit_duration_start.csv')
    df_end = read_csv_without_duplicates(filepath + '/block_commit_duration_end.csv')
    if len(df_start) <= 0 or len(df_end) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    # 重命名列
    df_start = df_start.rename(columns={'measure_time': 'start_time'})
    df_end = df_end.rename(columns={'measure_time': 'end_time'})
    # 合并df_start和df_end
    df = pd.merge(df_start, df_end, on='block_height')
    # 对于相同高度的区块，可能多次触发该节点准备打包，只保留最后一次记录时间
    df.drop_duplicates(subset='block_height', keep='last', inplace=True)
    # 计算时间差
    df['duration'] = df.apply(lambda row: calculate_duration(row['end_time'], row['start_time']), axis=1)
    df = df[df['duration'] >= 0]

    # 重命名'duration'列为'value'
    df.rename(columns={'duration': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(df["block_height"].tolist())
        .add_yaxis(series_name="当前节点出块耗时", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="当前节点出块耗时"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=45, range_end=55),
                         ],
                         xaxis_opts=opts.AxisOpts(name="区块高度"),
                         yaxis_opts=opts.AxisOpts(name="出块耗时/s"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '区块高度:'+ params.data.block_height + '</br>' +
                                             '区块哈希:'+ params.data.block_hash  + '</br>' +
                                             '块内交易数量:'+ params.data.block_tx_count  + '</br>' +
                                             '出块耗时:'+ params.data.value+ 's' ;
                                 }
                                 """
                             )
                         ),
                         )

    )
    chart, chart_js = parse_html(bar.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# 块内交易吞吐量
@app.route("/TxInBlockTps")
def get_tx_in_block_tps():
    # 读取csv文件
    df_start = read_csv_without_duplicates(filepath + '/tx_in_block_tps.csv')  # 打包完成时刻
    df_end = read_csv_without_duplicates(filepath + '/block_commit_duration_end.csv')  # 区块落库时刻
    if len(df_start) <= 0 or len(df_end) <= 0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")
    # 重命名列
    df_start = df_start.rename(columns={'measure_time': 'start_time'})
    df_end = df_end.rename(columns={'measure_time': 'end_time'})
    # 按照block_txsroot合并df_start和df_end
    df = pd.merge(df_start, df_end, on='block_txsroot')
    # 计算时间差
    df['duration'] = df.apply(lambda row: calculate_duration(row['end_time'], row['start_time']), axis=1)
    df = df[df['duration'] >= 0]
    # 计算块内吞吐量 = 块内交易数 / 耗时
    df['block_tps'] = df['block_tx_count_x'] / df['duration']

    # 重命名'block_tps'列为'value'
    df.rename(columns={'block_tps': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(df["block_height_x"].tolist())
        .add_yaxis(series_name="块内交易吞吐量", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="块内交易吞吐量"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=45, range_end=55),
                         ],
                         xaxis_opts=opts.AxisOpts(name="区块高度"),
                         yaxis_opts=opts.AxisOpts(name="块内交易吞吐量(笔/s)"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '区块高度:'+ params.data.block_height_x + '</br>' +
                                             '区块哈希:'+ params.data.block_hash  + '</br>' +
                                             '块内交易数量:'+ params.data.block_tx_count_x  + '</br>' +
                                             '块内交易树根:'+ params.data.block_txsroot  + '</br>' +
                                             '打包到落库耗时:'+ params.data.duration+ 's'+ '</br>' +
                                             '块内交易吞吐量:'+ params.data.value+ '笔/s' ;
                                 }
                                 """
                             )
                         ),
                         )

    )
    chart, chart_js = parse_html(bar.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


# 区块验证效率
@app.route("/BlockValidationEfficiency")
def get_block_validation_efficiency():
    # 读取csv文件
    df_duration = read_csv_without_duplicates(filepath + '/block_validation_efficiency_start.csv')
    df_cnt = read_csv_without_duplicates(filepath + '/block_validation_efficiency_end.csv')
    if len(df_duration) <= 0 or len(df_cnt) <=0:
        return render_template('draw.html', chart="<h2>当前文件尚无数据</h2>")

    df = pd.merge(df_duration, df_cnt, on='block_hash')
    # 应用转换函数到'block_validation_duration'列
    df['block_validation_duration'] = df['block_validation_duration'].apply(convert_duration_to_seconds)
    df = df[df['block_validation_duration'] >= 0]

    # 计算区块验证效率 = 块内交易数 / 验证耗时
    df['valid_efficiency'] = df['block_tx_count'].astype(float) / df['block_validation_duration']
    # 重命名'valid_efficiency'列为'value'
    df.rename(columns={'valid_efficiency': 'value'}, inplace=True)
    # 将新的DataFrame转换为字典列表
    data = df.to_dict('records')
    # 获取value中的最大值最小值
    min_value, max_value = 0, 100
    if len(data) != 0:
        min_value = float(min(data, key=lambda x: x['value'])['value'])
        max_value = float(max(data, key=lambda x: x['value'])['value'])
    bar = (
        Bar()
        .add_xaxis(df["block_height"].tolist())
        .add_yaxis(series_name="区块验证效率", y_axis=data)
        .set_global_opts(title_opts=opts.TitleOpts(title="区块验证效率"),
                         toolbox_opts=opts.ToolboxOpts(),
                         visualmap_opts=opts.VisualMapOpts(
                             min_=min_value,
                             max_=max_value
                         ),
                         datazoom_opts=[
                             opts.DataZoomOpts(range_start=45, range_end=55),
                         ],
                         xaxis_opts=opts.AxisOpts(name="区块高度"),
                         yaxis_opts=opts.AxisOpts(name="区块验证效率(笔/s)"),
                         tooltip_opts=opts.TooltipOpts(
                             formatter=JsCode(
                                 """
                                 function (params) {
                                     console.log(params);
                                     return '区块高度:'+ params.data.block_height + '</br>' +
                                             '块内交易数量:'+ params.data.block_tx_count  + '</br>' +
                                             '区块验证耗时:'+ params.data.block_validation_duration+ 's'+ '</br>' +
                                             '区块验证效率:'+ params.data.value+ '笔/s' ;
                                 }
                                 """
                             )
                         ),
                         )

    )
    chart, chart_js = parse_html(bar.render_embed())
    return render_template('draw.html', chart=chart, chart_js=chart_js)


if __name__ == "__main__":
    app.run(debug=True)
