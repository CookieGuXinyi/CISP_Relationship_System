1. 不同任务id，可能会产生同样的血缘关系，应该分开存储还是合并？
2. 当前main分支，只存储字段的直接计算语句full_expression（包含case when逻辑），忽略了group by、having、where等分组筛选逻辑。 ——  fix_parse分支尝试解决此问题，待完善。
