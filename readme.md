# SQL graph tool
A tool that analizes provided SQL script and builds a graph of table dependencies. 
This can be usefil when you have a complex SQL and you need to quickly identify which tables act as a source for the target table.

# Supported SQL-syntax
The script works with any ANSI SQL compatible syntax. Supports CTAS, CTE and temporary tables. Support fully and partially qualified table names.

# Planned improvements
Using of Jinja templates in (qualified) table names
