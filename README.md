# 飞书表格函数面板

跨飞书表格的可视化函数操作工具，支持 VLOOKUP / XLOOKUP / INDEX-MATCH / SUMIF / COUNTIF / SUMIFS。

## 功能特性

- 可视化配置函数参数，无需手写公式
- 支持跨多个飞书表格进行查找（XLOOKUP / INDEX-MATCH）
- 5 种匹配模式：精确、包含、前缀、后缀、正则
- 结果本地 SQLite 缓存，离线可用
- 一键导出 Excel（自动标色匹配/未匹配）
- 兼容企业内网代理 + 飞书开放平台 API 双模式

## 安装与运行

```bash
python feishu_vlookup_panel.py
```

依赖（`requests`、`openpyxl`）首次运行自动安装。

## 支持函数

| 函数 | 说明 |
|------|------|
| VLOOKUP | 垂直查找，支持多列返回 |
| XLOOKUP | 搜索列与返回列可来自不同飞书表格 |
| INDEX/MATCH | 最灵活的跨表查找 |
| SUMIF | 条件求和 |
| COUNTIF | 条件计数 |
| SUMIFS | 多条件（最多 4 个）求和 |

## 数据存储

缓存数据保存于 `feishu_vlookup_data/cache.db`（SQLite）。
