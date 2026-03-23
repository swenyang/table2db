# table2db 设计文档

## 概述

一个 Python 库，将任意 Excel 文件（`.xlsx` / `.xlsm` / `.xls`）及 CSV/TSV 文件自动解析为规整、可靠的 SQLite 数据库。处理 Excel 中常见的各种复杂情况（合并单元格、多级表头、小计行、类型混杂、错误值等），提供干净的数据供后端服务查询。

**场景**：用户上传任意 Excel/CSV → 系统自动分析结构并入库 → 返回临时 SQLite DB → 会话结束后清理
**集成方式**：作为独立 Python 库，后端（FastAPI）开发者自行集成调用

---

## 功能总览

### ✅ v1 支持

| 类别 | 功能 |
|------|------|
| **文件格式** | `.xlsx` / `.xlsm`（openpyxl）、`.xls`（兼容，xlrd）、`.csv` / `.tsv`（内置 csv 模块，自动检测分隔符和编码） |
| **流输入** | 同时接受文件路径（`str`）和类文件对象（`BinaryIO` / `BytesIO` / `UploadFile.file`）；流输入时需提供 `file_name` 参数以检测格式 |
| **多 Sheet** | **每个 Sheet → 一张独立 SQLite 表**；自动规范化表名、处理名称冲突 |
| **合并单元格** | 自动检测并展开，左上角值填充到所有被合并格 |
| **表头识别** | 自动定位表头行（支持偏移/非 A1 起始）；多级表头合并为 `一级_二级` |
| **小计/总计过滤** | 三信号加权检测（关键词 + 求和验证 + 样式）；内置中英文关键词，可扩展 |
| **类型推断** | 多数决（≥80%）；支持 INTEGER / REAL / TEXT / DATE；数字存为文本自动修正；布尔→整数 |
| **错误值处理** | `#REF!`, `#N/A`, `#DIV/0!` 等全部 → NULL |
| **公式处理** | 取计算结果值；未计算公式检测并 WARNING |
| **主键推断** | 列名模式 + 唯一性 + 非空检测 |
| **跨 Sheet 外键推断** | 列名匹配 + 值域包含验证（≥90%），基数保护（≥10 distinct） |
| **隐藏行/列/Sheet** | 隐藏 Sheet 默认跳过（可配置）；隐藏行/列保留数据但在元数据中标记 |
| **一 Sheet 多表** | 通过 island detection（`island_detector.detect_table_islands()` 连通域分析）自动检测一个 Sheet 中的多个独立表格，拆分为独立 DB 表 |
| **数据清洗** | 自动删除空行、去除完全重复行 |
| **DB 摘要** | 独立模块，生成 LLM 友好的 Markdown（表结构 + sample 数据 + 列级统计） |
| **异步支持** | `convert_async()` / `process_async()` 通过 `asyncio.to_thread()` 实现非阻塞 FastAPI 集成 |
| **可插拔 Loaders** | `BaseLoader` 抽象基类；默认 `SqliteLoader` 支持自定义输出路径；可自行实现其他数据库后端 |
| **生命周期管理** | 临时 SQLite 文件 + 上下文管理器自动清理 |
| **错误处理** | 完整异常层次（FileReadError / NoDataError / UnsupportedFormatError / SchemaError） |
| **可观测性** | 分 Stage 日志（Python logging）；处理过程 warnings 收集 |

### ❌ v1 明确不支持

| 排除项 | 说明 |
|--------|------|
| `.xlsb` 格式 | 二进制 Excel，需要额外依赖 |
| 密码保护文件 | 抛 `FileReadError`，无法解密读取 |

---

## 架构：Pipeline 管道

将解析过程拆分为 6 个有序 Stage，每个 Stage 只做一件事，消费和产出统一的中间数据结构 `WorkbookData`。

```
Excel 文件 / CSV / TSV / 流（BytesIO）
  → Stage 1: 原始读取 (reader)          — 解析单元格、合并区域、隐藏状态；CSV/TSV 自动检测分隔符与编码
  → Stage 2: 结构检测 (structure)        — 连通域检测（detect_table_islands）、定位表头、多表拆分
  → Stage 3: 数据清洗 (cleaner)
  → Stage 4: 类型推断 (typer)
  → Stage 5: 关系推断 (relator)
  → Stage 6: 入库 (loader)
  → ConversionResult (SQLite DB + 元数据)
```

**优点**：每阶段可独立测试/调试/替换；职责清晰；易扩展新 Stage。

**日志**：使用 Python `logging` 模块，每个 Stage 拥有独立 logger（如 `table2db.pipeline.reader`）。关键节点记录 INFO 日志（进入/退出 Stage、行数变化），异常情况记录 WARNING。调用者可通过标准 `logging.getLogger("table2db")` 控制级别。

---

## 错误处理

定义异常层次结构，所有异常继承自 `ExcelToDbError`：

```python
class ExcelToDbError(Exception): ...

class FileReadError(ExcelToDbError):
    """文件无法读取：损坏、密码保护、非 Excel 格式、文件不存在"""

class NoDataError(ExcelToDbError):
    """文件可读但无有效数据：所有 Sheet 为空或被清洗后无数据"""

class UnsupportedFormatError(ExcelToDbError):
    """不支持的文件格式（如 .xlsb 等）"""

class SchemaError(ExcelToDbError):
    """无法推断出有效表结构（如无法定位表头）"""
```

**原则**：可恢复的问题记 warning（如单个错误值 → NULL）；不可恢复的问题抛异常。

---

## 核心数据模型

```python
@dataclass
class CellCoord:
    row: int
    col: int

@dataclass
class ForeignKey:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence: float  # 0.0 ~ 1.0

@dataclass
class SheetData:
    name: str                              # Sheet 名称
    header_row_start: int                  # 表头起始行号
    header_row_end: int                    # 表头结束行号（单行表头时 == start）
    headers: list[str]                     # 列名列表（已去重、规范化；多级表头已合并为 "一级_二级"）
    rows: list[list[Any]]                  # 二维数据（不含表头）
    column_types: dict[str, str]           # 列名 → 推断类型 (TEXT/INTEGER/REAL/DATE)
    primary_key: str | None                # 推断出的主键列名（如有）
    excluded_rows: list[int]               # 被过滤的行号（小计/总计行等）
    merge_map: dict[tuple[CellCoord, CellCoord], Any]  # 合并单元格映射（Stage 1 填充，Stage 2 消费用于表头解析，之后仅保留作为元数据）
    metadata: dict                         # 附加信息（原始行数、错误统计、隐藏行/列信息等）

@dataclass
class WorkbookData:
    source_file: str
    sheets: list[SheetData]
    relationships: list[ForeignKey]        # 推断出的跨表关系

@dataclass
class TableInfo:
    name: str
    columns: list[dict]  # [{"name": str, "type": str}, ...]
    row_count: int
    source_sheet: str
    primary_key: str | None
    confidence: float  # island 检测置信度（单表 Sheet 为 1.0）

@dataclass
class ConversionResult:
    db_path: str                           # SQLite 文件路径
    tables: list[TableInfo]                # 表结构信息
    relationships: list[ForeignKey]        # 外键关系
    warnings: list[str]                    # 处理警告
    metadata: dict                         # 统计信息

    def cleanup(self): ...                 # 删除临时 .db 文件
    def __enter__(self): ...               # 上下文管理器支持
    def __exit__(self, *args): ...
```

---

## Stage 详细设计

### Stage 1 — 原始读取 (`pipeline/reader.py`)

**输入**：文件路径（`str`）或类文件对象（`BinaryIO` / `BytesIO`），流输入时需提供 `file_name` 参数  
**输出**：初始化的 `WorkbookData`（原始数据 + 合并单元格信息）  
**异常**：`FileReadError`（文件损坏/密码保护/不存在）、`UnsupportedFormatError`（不支持的格式）

- **输入类型**：接受 `Union[str, BinaryIO]`。当传入流（如 `BytesIO`、`UploadFile.file`）时，`file_name` 参数为必填，用于从扩展名判断文件格式。
- 使用 **openpyxl**（`data_only=True`）读取 `.xlsx` / `.xlsm`，取公式的计算结果而非公式文本
- **openpyxl 限制**：`data_only=True` 对未经 Excel 打开/保存的文件（如脚本生成的 xlsx）会返回 `None`。检测策略：如果某列 > 50% 的值为 `None` 但对应公式存在（通过 `data_only=False` 二次读取检查），记录 WARNING 提示用户"该文件含未计算的公式，建议先在 Excel 中打开保存后重新上传"
- `.xls` 格式 fallback 到 **xlrd**
- **CSV/TSV 支持**：`.csv` 和 `.tsv` 文件使用 Python 内置 `csv` 模块读取。支持自动**分隔符嗅探**（通过 `csv.Sniffer`）检测逗号、制表符、分号或管道符分隔符。**编码检测**优先尝试 UTF-8，然后回退到区域感知检测以支持其他编码（如 GBK、Latin-1）。
- 读取合并单元格信息 → `merge_map`：将合并区域的左上角值填充到所有被合并的单元格。适用于两种常见场景：
  - **标签合并**：如"华北区"跨 3 行 → 每行都填充"华北区"
  - **数字合并**：如小计 600 跨 3 行 → 每行都填充 600（冗余但不丢数据，后续 Stage 3 可过滤）
- 跳过隐藏 Sheet（默认行为，可通过 `skip_hidden_sheets=False` 关闭）
- **隐藏行/列**：读取但在 `metadata` 中标记 `hidden_rows` 和 `hidden_cols` 列表，默认保留数据（隐藏 ≠ 无效）
- Excel 错误值（`#REF!`, `#N/A`, `#DIV/0!`, `#VALUE!`, `#NAME?`, `#NULL!`）统一转为 `None`

### Stage 2 — 结构检测 (`pipeline/structure.py`)

**输入**：含原始数据的 `WorkbookData`  
**输出**：表头已定位、列名已规范化的 `WorkbookData`  
**异常**：`SchemaError`（无法定位任何表头）

**表头定位**启发式规则（从第 1 行开始扫描）：
1. 该行非空单元格占比 > 50%（相对于该 Sheet 的最大使用列数）— 可通过 `header_min_fill_ratio` 配置（默认 0.5）
2. 值类型 ≥ 70% 为字符串 — 可通过 `header_min_string_ratio` 配置（默认 0.7）
3. 该行下方连续 ≥ 3 行有数据（至少每行有 1 个非空单元格）
4. 跳过标题行（仅 1-2 个单元格有值，或合并单元格横跨整行 ≥ 80% 列）

**数据区域界定**：表头行以下到最后一个非空行（忽略尾部连续空行）

**列名规范化**：
- 去首尾空格、去换行符
- 重复列名加后缀 `_1`, `_2`
- 空列名用 `column_N` 占位

**多级表头处理**：
- 检测表头是否跨多行（如果紧接表头的下一行 ≥ 70% 单元格为字符串且不像数据行）
- 合并为 `"一级_二级"` 格式
- `header_row_start` 记录第一行，`header_row_end` 记录最后一行

**消费 `merge_map`**：利用合并单元格信息辅助识别多级表头中的跨列父级表头。Stage 2 之后 `merge_map` 仅作为元数据保留。

**一 Sheet 多表**：通过 `island_detector` 模块（`table2db.pipeline.island_detector`）支持。`detect_table_islands()` 函数对 Sheet 的非空单元格网格执行连通域分析，识别独立的数据区域（岛）。每个检测到的岛被提取为独立的表。`TableInfo.confidence` 携带 island 检测置信度分数（单表 Sheet 为 1.0，存在歧义拆分时分数较低）。如果仅发现一个岛，则按正常流程处理。

**空 Sheet 处理**：如果 Sheet 无法定位到有效表头或数据行，跳过该 Sheet 并记录 WARNING。

### Stage 3 — 数据清洗 (`pipeline/cleaner.py`)

**输入**：结构已检测的 `WorkbookData`  
**输出**：清洗后的 `WorkbookData`（已过滤汇总行、空行、重复行）

**小计/总计行检测**（多信号加权打分，默认权重与阈值）：
1. **关键词匹配**（权重 0.5）：行中文本单元格包含 `合计|小计|总计|总价|sum|total|subtotal|grand total`
   - 内置中英文关键词，支持开发者通过 `subtotal_keywords` 参数扩展
   - 不区分大小写，支持关键词中间有空格（如 `"合 计"`）
2. **结构信号**（权重 0.3）：该行数值列的值等于上方连续数据区域的合
3. **样式信号**（权重 0.2）：行格式与数据行不同（加粗、背景色等，通过 openpyxl 样式读取）

加权分数 ≥ 0.5 则判定为汇总行并过滤。被过滤的行号记录到 `excluded_rows`。

**多语言支持**：内置中文 + 英文关键词默认覆盖主流场景；结构信号作为语言无关的兜底。

**其他清洗**：
- 整行全空 → 删除
- 完全重复的行 → 去重（保留首次出现）

**清洗后空表处理**：如果某个 Sheet 清洗后无数据行，跳过该 Sheet 并记录 WARNING `"Sheet '{name}' has no data rows after cleaning, skipped"`。

### Stage 4 — 类型推断 (`pipeline/typer.py`)

**输入**：清洗后的 `WorkbookData`  
**输出**：带类型信息的 `WorkbookData`

对每列的非空值进行类型采样，分类为 SQLite 兼容类型：`INTEGER`, `REAL`, `TEXT`, `DATE`

> 注意：Excel 中的布尔值（TRUE/FALSE）映射为 `INTEGER`（1/0），不单独作为类型。

**多数决规则**：
- **数字合并计数**：INTEGER 和 REAL 合并为"数字类"参与阈值判断。如数字类占比 ≥ 80%（可通过 `type_threshold` 配置），则判定为数字列；有任何 REAL 值存在则选 REAL，否则选 INTEGER。
- 非数字类型（TEXT, DATE）独立计算占比，≥ 80% 则采用。
- 均不满足则 fallback 为 `TEXT`。

> 设计考量：`"51.0"` 这样的字符串，`float("51.0")` 后因 `51.0 == int(51.0)` 会被归为 INTEGER，而 `"12.75"` 归为 REAL。如果不合并计数，同一列的 REAL 和 INTEGER 会互相稀释，导致本应是数字的列 fallback 为 TEXT。

**特殊处理**：
- **数字存为文本**：值是字符串但 `float(val)` 不报错 → 视为数字
- **日期检测**：openpyxl 的 `datetime` 类型 + 常见日期字符串模式匹配（`YYYY-MM-DD`, `YYYY/MM/DD`, `DD/MM/YYYY` 等）
- **布尔值**：Python `bool` 或字符串 `"TRUE"/"FALSE"` → 转为 `INTEGER` 1/0
- **类型不匹配的值**：尝试转换，转换失败则置 `None`

### Stage 5 — 关系推断 (`pipeline/relator.py`)

**输入**：带类型信息的 `WorkbookData`  
**输出**：含 `relationships` 和 `primary_key` 的 `WorkbookData`

#### 5a. 主键推断

对每张表的每列评估是否为主键候选：
- 列名匹配模式：`id`, `*_id`, `*_no`, `*_code`（权重 +0.3）
- 值 100% 唯一且 100% 非空（必要条件）
- 类型为 `INTEGER` 或 `TEXT`（排除 REAL/DATE）
- 如果有多个候选列，取第一个名为 `id` 的列；否则取最左的候选列

#### 5b. 外键推断

**列名匹配**：
- 完全相同的列名出现在两张表中（如 `customer_id`）
- 命名模式匹配（表 A 有 `id` 列，表 B 有 `a_id` 列，其中 `a` 是表 A 的名称或缩写）

**值域验证**：
- B 表该列的值集合 ⊆ A 表候选主键列的值集合
- 允许 ≥ 90% 包含率（容忍少量脏数据）
- **基数保护**：候选主键列必须有 ≥ 10 个 distinct 值（避免小整数列的误匹配，如 1-5 的评分列）
- 排除两张表同列名但两边都是主键的情况（那是同名非关联）

**输出**：`ForeignKey(from_table, from_col, to_table, to_col, confidence)` 列表  
**默认阈值**：仅 `confidence ≥ 0.8` 的才建立实际 SQL 外键约束。可通过 `fk_confidence_threshold` 配置。

### Stage 6 — 入库 (`pipeline/loader.py`)

**输入**：完整的 `WorkbookData`  
**输出**：`ConversionResult`（含 SQLite DB 路径）

- 在 `tempfile.mkdtemp()` 下创建 SQLite 文件
- **表名规范化**：Sheet 名 → 去特殊字符 → 转 snake_case → 去重（重复名加后缀 `_1`, `_2`）
- **标识符安全**：所有表名和列名在 DDL 中使用 SQLite 双引号包裹（`"table_name"`），防止特殊字符或保留字导致语法错误。额外过滤控制字符和 NUL 字节。
- 根据 `column_types` 生成 `CREATE TABLE` DDL
- 如有 `primary_key`，添加 `PRIMARY KEY` 约束
- **外键约束写入 DDL**：`FOREIGN KEY ("col") REFERENCES "other_table" ("col")`。被引用表先创建，确保引用顺序正确。同时启用 `PRAGMA foreign_keys = ON`。
- 使用 `executemany` 批量插入数据
- 创建元数据表 `_meta`：原始文件名、每张表的源 Sheet、行数、列数、类型推断统计、**外键关系及置信度**
- `ConversionResult.relationships` 中的表名统一为规范化后的名称（与 DDL 一致）

---

## DB 摘要模块 (`describe.py`)

独立于 pipeline 的可选模块，用于生成 LLM 友好的数据库描述。

**输入**：`ConversionResult`  
**输出**：Markdown 格式字符串

**包含内容**：
- 各表描述（表名、列名 + 类型、行数、主键/外键）
- 每表前 N 行 sample 数据（默认 3 行，Markdown 表格格式）
- 列级统计：
  - 数值列：min / max / avg / null 率 / distinct count
  - 文本列：null 率 / distinct count / top 3 值及频次
- 跨表关系描述（from.col → to.col）

```python
from table2db.describe import generate_db_summary

summary: str = generate_db_summary(result, sample_rows=3)
# 返回 Markdown 格式文本，可直接作为 LLM context
```

**测试方式**：`tests/test_describe.py` 通过构造内存中的 SheetData → load_to_sqlite → generate_db_summary，验证输出 Markdown 包含：
- 表名、列名、列类型
- sample 数据值
- 数值列统计（min/max）
- null 百分比
- FK 关系符号（`→`）
- sample_rows 参数生效

**FastAPI 集成示例**：
```python
@app.post("/upload")
async def upload(file: UploadFile):
    converter = TableConverter()
    result = converter.convert(saved_path)
    summary = generate_db_summary(result, sample_rows=5)
    # summary 可直接发给 LLM 作为数据库 schema context
    return {"tables": [t.name for t in result.tables], "summary": summary}
```

---

## 公开 API

```python
from table2db import TableConverter
from table2db.errors import FileReadError, NoDataError

# 基础用法
converter = TableConverter()
result = converter.convert("data.xlsx")

result.db_path          # str: SQLite 文件路径
result.tables           # list[TableInfo]
result.relationships    # list[ForeignKey]
result.warnings         # list[str]
result.metadata         # dict

# 从类文件对象读取（如 FastAPI UploadFile）
import io
with open("data.xlsx", "rb") as f:
    stream = io.BytesIO(f.read())
result = converter.convert(stream, file_name="data.xlsx")

# 两阶段用法：仅执行 Stage 1-5，然后单独加载
workbook_data, warnings = converter.process("data.xlsx")
# 加载前检查中间结果
for sheet in workbook_data.sheets:
    print(f"{sheet.name}: {len(sheet.headers)} 列, {len(sheet.rows)} 行")

# 异步支持（适用于 FastAPI / asyncio）
result = await converter.convert_async("data.xlsx")
wb, warnings = await converter.process_async("data.xlsx")

# 查询
import sqlite3
conn = sqlite3.connect(result.db_path)
rows = conn.execute("SELECT * FROM orders LIMIT 10").fetchall()

# 清理
result.cleanup()

# 上下文管理器
with converter.convert("data.xlsx") as result:
    conn = sqlite3.connect(result.db_path)
    # ...
# 退出自动清理

# 错误处理
try:
    result = converter.convert("bad_file.xlsx")
except FileReadError as e:
    print(f"文件无法读取: {e}")
except NoDataError as e:
    print(f"文件无有效数据: {e}")

# 可配置项
converter = TableConverter(
    subtotal_keywords=["合计", "Total", ...],  # 扩展小计关键词
    type_threshold=0.8,                         # 类型多数决阈值（默认 0.8）
    skip_hidden_sheets=True,                    # 是否跳过隐藏 Sheet（默认 True）
    fk_confidence_threshold=0.8,                # 外键置信度阈值（默认 0.8）
    header_min_fill_ratio=0.5,                  # 表头行非空单元格最小占比（默认 0.5）
    header_min_string_ratio=0.7,                # 表头行字符串单元格最小占比（默认 0.7）
)
```

---

## 项目结构

```
table2db/                          # 项目根目录
├── pyproject.toml
├── README.md
├── table2db/                      # Python 包
│   ├── __init__.py                # 导出 TableConverter, ConversionResult
│   ├── converter.py               # TableConverter 主类，编排 pipeline
│   ├── models.py                  # 数据模型
│   ├── errors.py                  # 异常层次结构
│   ├── cli.py                     # CLI 入口
│   ├── describe.py                # DB 摘要生成
│   ├── loaders/
│   │   ├── base.py                # BaseLoader 抽象基类
│   │   └── sqlite_loader.py       # SqliteLoader
│   └── pipeline/
│       ├── reader.py              # Stage 1: 原始读取
│       ├── structure.py           # Stage 2: 结构检测
│       ├── island_detector.py     # 一 Sheet 多表检测
│       ├── cleaner.py             # Stage 3: 数据清洗
│       ├── typer.py               # Stage 4: 类型推断
│       ├── relator.py             # Stage 5: 关系推断
│       └── loader.py              # Stage 6: 入库（薄包装层）
└── tests/
    ├── conftest.py
    ├── generate_fixtures.py       # 自动生成测试 fixture
    ├── generate_outputs.py        # 为所有 fixture 生成 .db + _summary.md
    └── test_*.py                  # 每个 stage + 集成测试
```

## 依赖

- `openpyxl` — 读 `.xlsx`（合并单元格、样式、公式计算值）
- `xlrd` — 读 `.xls`（老格式 fallback）
- `sqlite3` — 入库（Python 内置）
- `tempfile` — 临时文件管理（Python 内置）
- `logging` — 日志（Python 内置）
- `pytest` — 测试框架（dev 依赖）

---

## 测试策略

### 程序化构造 Fixtures（主要）

使用 `tests/generate_fixtures.py` 通过 openpyxl 程序化生成测试 Excel 文件，精确控制每种边界情况：

| Fixture 文件 | 验证场景 |
|---|---|
| `simple.xlsx` | 基线：干净表格，验证正常流程 |
| `merged_cells.xlsx` | 合并单元格（表头合并、数据区域合并） |
| `multi_header.xlsx` | 多级表头（2-3 行嵌套） |
| `subtotals.xlsx` | 小计/总计/合计行过滤（中英文） |
| `mixed_types.xlsx` | 同一列类型混杂（数字 + 文本 + 空值） |
| `error_values.xlsx` | Excel 错误值 (#REF!, #N/A 等) |
| `offset_table.xlsx` | 数据不从 A1 开始 |
| `empty_gaps.xlsx` | 数据中间穿插空行/空列 |
| `multi_sheet_fk.xlsx` | 多 Sheet + 跨表外键关系 |
| `number_as_text.xlsx` | 数字存为文本格式 |
| `dates_mixed.xlsx` | 多种日期格式混合 |
| `real_world_dirty.xlsx` | 综合：多种问题叠加 |
| `hidden_rows_cols.xlsx` | 隐藏行和隐藏列 |
| `empty_after_clean.xlsx` | 清洗后无数据（全是汇总行） |
| `duplicate_sheet_names.xlsx` | 多 Sheet 名规范化后冲突 |

### 测试分层

- **单元测试**：每个 Stage 独立测试（输入 → 输出断言）
- **集成测试**：完整 pipeline 端到端（Excel → SQLite → 查询验证）
- **异常测试**：损坏文件、空文件、非 Excel 文件 → 正确抛异常
- **回归测试**：真实世界文件不会崩溃

---

## Excel 复杂情况处理原则

**结构层面**（全面处理）：合并单元格、多级表头、数据偏移（不从 A1 开始）、多 Sheet 转多表、跨 Sheet 外键、隐藏行/列/Sheet

**数据层面**（重点处理）：公式单元格（取计算值）、未计算的公式（warning）、错误值、空行/空列穿插、手动小计/总计行、类型混杂、数字存为文本、日期格式不一致、布尔值、隐式类型转换、重复行

**格式层面**（忽略，不影响数据）：数据验证（下拉框）、条件格式、命名区域、批注/注释、嵌入对象
