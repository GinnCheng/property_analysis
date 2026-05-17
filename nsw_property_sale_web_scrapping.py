import io
import os
import re
import shutil
import zipfile
from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv

# 加载环境配置
load_dotenv()
PARQUET_PATH = os.getenv("PARQUET_PATH", "nsw_property_sales.parquet")
TEMP_DIR = os.getenv("TEMP_DIR", "nsw_sales_temp")


def parse_nsw_valnet_data(raw_data):
    """【核心解析引擎】解析单块分号分割的 DAT 文本，横向聚合 B、C 行"""
    if isinstance(raw_data, bytes):
        raw_data = raw_data.decode("utf-8", errors="ignore")

    records = []
    current_b = None

    for line in io.StringIO(raw_data):
        line = line.strip()
        if not line:
            continue
        parts = line.split(";")
        type_flag = parts[0]

        if type_flag == "B":
            if current_b:
                records.append(current_b)
            current_b = {
                "property_id": parts[2] if len(parts) > 2 else None,
                "house_number": parts[7] if len(parts) > 7 else None,
                "street_name": parts[8] if len(parts) > 8 else None,
                "suburb": parts[9] if len(parts) > 9 else None,
                "postcode": parts[10] if len(parts) > 10 else None,
                "area": float(parts[11]) if len(parts) > 11 and parts[11] else None,
                "area_unit": parts[12] if len(parts) > 12 else None,
                "contract_date": parts[13] if len(parts) > 13 else None,
                "settlement_date": parts[14] if len(parts) > 14 else None,
                "sale_price": (
                    float(parts[15]) if len(parts) > 15 and parts[15] else None
                ),
                "zone": parts[16] if len(parts) > 16 else None,
                "property_type": parts[18] if len(parts) > 18 else None,
                "lot_description": "",
            }
        elif type_flag == "C" and current_b:
            if len(parts) > 5 and parts[2] == current_b["property_id"]:
                current_b["lot_description"] = parts[5]

    if current_b:
        records.append(current_b)
    return records


def process_zip_stream(zip_bytes, week_str):
    """【内存流解压】解压 Weekly ZIP 字节流，就地解析内部的所有 DAT 文件"""
    weekly_records = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for file_name in z.namelist():
            if file_name.upper().endswith(".DAT"):
                with z.open(file_name) as dat_file:
                    dat_content = dat_file.read()
                    file_records = parse_nsw_valnet_data(dat_content)
                    for r in file_records:
                        r["source_week"] = week_str  # 附带元数据追溯标签
                    weekly_records.extend(file_records)
    return weekly_records


def fetch_available_dates_from_portal():
    """【双保险爬虫】从官网 HTML 解析 Weekly 日期。

    若遭遇网络错误或反爬，自动依据时间轴算法动态推算当年元旦至今天的所有周一。
    """
    url = "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()

        # 正则匹配页面按钮文本中的日期，如 "05 Jan 2026"
        date_matches = re.findall(r"\d{2}\s+[A-Za-z]{3}\s+\d{4}", res.text)
        if not date_matches:
            raise ValueError("未能从页面解析出任何日期标签")

        standard_dates = []
        for d_str in set(date_matches):
            dt = datetime.strptime(d_str, "%d %b %Y")
            standard_dates.append(dt.strftime("%Y%m%d"))
        return sorted(standard_dates)

    except Exception as e:
        print(f"\n⚠️ 无法从官网动态解析日期列表 ({e})")
        print("⚙️ 触发智能时间轴兜底机制：正在动态计算今年以来的所有周一...")

        # 动态推算今年以来直至今天的所有周一
        today = datetime.now()
        start_date = datetime(today.year, 1, 1)

        # 寻找当年的第一个周一
        days_ahead = 0 - start_date.weekday()
        if days_ahead < 0:
            days_ahead += 7
        first_monday = start_date + pd.Timedelta(days=days_ahead)

        computed_mondays = []
        iter_date = first_monday
        while iter_date <= today:
            computed_mondays.append(iter_date.strftime("%Y%m%d"))
            iter_date += pd.Timedelta(weeks=1)

        print(
            f"📅 成功推算出 {today.year} 年 {computed_mondays[0]} 至 {computed_mondays[-1]} 的全部周一序列，共 {len(computed_mondays)} 个。"
        )
        return computed_mondays


def run_nsw_sales_pipeline(download_annual=False, start_year=1990, end_year=2025):
    """【主数据管道】调度、增量去重对比、落盘 Parquet 备份并百分之百返回最新全量 DF"""
    os.makedirs(TEMP_DIR, exist_ok=True)
    all_extracted_records = []
    processed_weeks = set()
    existing_df = None  # 显式全局初始化

    # 检查本地是否有历史 Parquet 缓存
    if os.path.exists(PARQUET_PATH):
        print(f"📂 发现本地已存的 Parquet 仓库: {PARQUET_PATH}")
        existing_df = pd.read_parquet(PARQUET_PATH)
        if "source_week" in existing_df.columns:
            processed_weeks = set(existing_df["source_week"].unique())
            print(f"ℹ️ 检测到已存在 {len(processed_weeks)} 个周的数据，进入【增量更新】模式。")
    else:
        print("🆕 未发现本地仓库，将初始化构建。")

    # ---- 阶段 1: 处理 Annual 历史年包 (仅在初始化或者显式开启时执行) ----
    if download_annual and not processed_weeks:
        print("🚀 开始处理历史 Annual 销售档案包...")
        for year in range(start_year, end_year + 1):
            annual_url = f"https://www.valuergeneral.nsw.gov.au/__psi/yearly/{year}.zip"
            print(f"📥 正在下载 {year} 全年大包: {annual_url}")
            res = requests.get(annual_url, stream=True)
            if res.status_code == 200:
                annual_zip_path = os.path.join(TEMP_DIR, f"{year}.zip")
                with open(annual_zip_path, "wb") as f:
                    f.write(res.content)

                with zipfile.ZipFile(annual_zip_path) as az:
                    for sub_file in az.namelist():
                        if sub_file.endswith(".zip"):
                            week_str = os.path.basename(sub_file).replace(".zip", "")
                            if week_str in processed_weeks:
                                continue
                            print(f"   📦 正在剥离年包中的周数据: {week_str}")
                            sub_zip_bytes = az.read(sub_file)
                            week_data = process_zip_stream(sub_zip_bytes, week_str)
                            all_extracted_records.extend(week_data)
                            processed_weeks.add(week_str)
                os.remove(annual_zip_path)

    # ---- 阶段 2: 处理 Weekly 实时周包 (增量对比逻辑) ----
    print("🚀 扫描网页最新的 Weekly 增量更新区...")
    web_weekly_dates = fetch_available_dates_from_portal()

    for week_str in web_weekly_dates:
        if week_str in processed_weeks:
            print(f"✅ 周数据 {week_str} 已存在于本地仓库，跳过下载。")
            continue

        weekly_url = f"https://www.valuergeneral.nsw.gov.au/__psi/weekly/{week_str}.zip"
        print(f"📥 发现新周数据！正在捕获: {weekly_url}")
        res = requests.get(weekly_url)
        if res.status_code == 200:
            week_data = process_zip_stream(res.content, week_str)
            all_extracted_records.extend(week_data)
            processed_weeks.add(week_str)
        else:
            # 兼容处理推算出来的未来周一导致官网返回 404 的情况
            print(f"⚠️ 链接暂不可用 (状态码 {res.status_code}): {weekly_url}")

    # ---- 阶段 3: 数据整合、类型转换与 Parquet 闪存持久化 ----
    return_df = pd.DataFrame()  # 安全返回容器

    if all_extracted_records:
        new_df = pd.DataFrame(all_extracted_records)

        # 格式化日期类型
        for date_col in ["contract_date", "settlement_date"]:
            new_df[date_col] = pd.to_datetime(
                new_df[date_col], format="%Y%m%d", errors="coerce"
            )

        # 内存安全拼接，绝不二次读盘
        if existing_df is not None:
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            # 全局跨文件去重
            final_df.drop_duplicates(
                subset=["property_id", "contract_date", "sale_price"],
                inplace=True,
            )
        else:
            final_df = new_df

        # 闪存持久化备份
        final_df.to_parquet(
            PARQUET_PATH, index=False, compression="snappy", engine="pyarrow"
        )
        print(f"🎉 成功持久化到本地！当前仓库总行数: {len(final_df)}")
        return_df = final_df
    else:
        print("🔔 检查完毕：当前本地仓库已经是最新状态，无需写入。")
        # 无新数据时，原地直接吐出开头加载好的老数据内存对象
        return_df = existing_df if existing_df is not None else pd.DataFrame()

    # ---- 阶段 4: 强制销毁 Temp 垃圾缓存 ----
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
        print("🗑️ 临时文件夹已被完全强制粉碎。")

    return return_df


# ==========================================
# 独立测试 / 调用入口
# ==========================================
if __name__ == "__main__":
    # 第一次运行想拉取 1990-2025 全量历史年包时，把 download_annual 设为 True
    df = run_nsw_sales_pipeline(download_annual=False)

    print("\n" + "=" * 60)
    print(f"🚀 数据管道运行结束！已成功捕获并返回最新完整 DataFrame。")
    print(f"📊 内存中当前全量数据矩阵行数: {df.shape[0]}, 列数: {df.shape[1]}")
    print("=" * 60)