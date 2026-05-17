import json
import pandas as pd


def auto_json_to_df(json_data, record_key="results"):
    """通用函数：自动观察 JSON 结构，打平所有嵌套的 keys 并转换为 DataFrame

    :param json_data: 原始数据，支持 bytes 字符串、str 字符串、dict 或 list
    :param record_key: 如果输入是字典，指定包含核心数据的那个 key（例如 'results'）
    """
    # 1. 自动转换数据类型，确保最后拿到的是 list
    if isinstance(json_data, bytes):
        json_data = json_data.decode("utf-8")
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    if isinstance(json_data, dict):
        records = json_data.get(record_key, [])
    elif isinstance(json_data, list):
        records = json_data
    else:
        raise ValueError("不支持的数据类型")

    # 2. 内部递归函数：通过观察 keys 自动打平嵌套结构
    def flatten_dict(d, parent_key="", sep="_"):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                # 如果是字典，递归打平
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if all(isinstance(i, dict) for i in v):
                    # 如果是 [{...}, {...}] 结构（如 agency, agents），把它转成字符串
                    # 或者你也可以只取第一个元素，这里默认转成 json 串保留所有元数据
                    items.append((new_key, json.dumps(v, ensure_ascii=False)))
                else:
                    # 如果是普通列表（如 coordinates, inspections），直接保留
                    items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)

    # 3. 循环处理每一条记录
    flattened_records = [flatten_dict(rec) for rec in records]

    # 4. 生成 DataFrame
    return pd.DataFrame(flattened_records)





def universal_json_to_df(data_input):
    """真正的通用函数：无论 JSON 嵌套多深、核心列表藏在哪个 Key 下面，

    自动动态扫描所有 Keys 并提取转换为 DataFrame（绝不覆盖你的 data 变量）。
    """
    # 1. 自动转换数据类型（兼容 bytes/str/dict）
    if isinstance(data_input, bytes):
        data_input = json.loads(data_input.decode("utf-8"))
    elif isinstance(data_input, str):
        data_input = json.loads(data_input)

    # 2. 核心递归逻辑：负责把嵌套字典彻底打平（如 location_coordinate_lat）
    def flatten_dict(d, parent_key="", sep="_"):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list) and all(isinstance(i, dict) for i in v):
                # 针对类似 agents/advertisers 这种嵌套的对象列表，直接转 json 串保留
                items.append((new_key, json.dumps(v, ensure_ascii=False)))
            else:
                items.append((new_key, v))
        return dict(items)

    # 3. 广度/深度探测引擎：自动在 JSON 树里掘进，寻找所有可用的“房源记录”
    all_rows = []

    def extract_records(node, source_tag=""):
        if isinstance(node, list):
            for item in node:
                if isinstance(item, dict):
                    flat_item = flatten_dict(item)
                    if source_tag:
                        flat_item["api_source_group"] = source_tag
                    all_rows.append(flat_item)
        elif isinstance(node, dict):
            # 关键：检查当前字典的所有分支
            for key, val in node.items():
                # 如果分支是个列表，比如 'similar' 或 'results'
                if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                    extract_records(val, source_tag=key)
                # 如果是个特定的独立业务字典，比如第一个 API 里的 'subject'
                elif (
                    key == "subject"
                    and isinstance(val, dict)
                    and "property_id" in val
                ):
                    flat_item = flatten_dict(val)
                    flat_item["api_source_group"] = "subject"
                    all_rows.append(flat_item)
                # 继续向更深层探测
                elif isinstance(val, (dict, list)):
                    extract_records(val, source_tag=source_tag)

    # 执行扫描
    extract_records(data_input)

    # 4. 转换为 DataFrame
    return pd.DataFrame(all_rows)


