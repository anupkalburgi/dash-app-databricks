# brick_sqlalchemy.py
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, select, Column, Date, Float, Integer, String, text, and_
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, select, asc, desc, func
import pandas as pd


load_dotenv()


SERVER_HOSTNAME = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH")
ACCESS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

CATALOG = os.environ.get("DATABRICKS_CATALOG")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA")

connection_string = f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"




class BrickSQLAlchemy:
    """
    A simple example class that uses SQLAlchemy to:
      1) Discover table schema (column names, etc.).
      2) Query data from a table with optional offset/limit/sort/group/aggregate.

    Usage:
       brick = BrickSQLAlchemy("postgresql://user:pw@host:port/dbname")
       schema_info = brick.get_schema_for_table("catalog", "public", "mytable")
       data_df     = brick.get_data_query("catalog", "public", "mytable", ...)
    """

    def __init__(self, connection_string):
        # Create your engine with the provided connection string
        # E.g. "postgresql://user:pw@localhost:5432/mydb"
        self.engine = create_engine(connection_string, echo=True)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    

    def test_connection(self):
        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT 1")
                print("Connection OK, SELECT 1 =>", result.scalar())
        except Exception as e:
            print("Connection failed:", e)

    def get_table_names(self):
        """
        Return a list of table names in the database.
        """
        return self.metadata.tables.keys()

    def _internal_schema(self, table_name):
        """
        Reflects the given table and returns an object with a .fields attribute,
        each 'field' having a 'name' property.
        For convenience, returns a simple namedtuple-like object.
        """
        # Reflect the table from the database
        table = self.metadata.tables.get(table_name)
        return table

    def get_schema_for_table(self, table_name):
        """
        Reflects the given table and returns an object with a .fields attribute,
        each 'field' having a 'name' property.
        For convenience, returns a simple namedtuple-like object.
        """
        from collections import namedtuple

        
        # Reflect the table from the database
        table = self._internal_schema(table_name)
        Field = namedtuple("Field", ["name"])
        # Build the list of field metadata
        fields = [Field(col.name) for col in table.columns]
        print(f"Bricks getting schema {table_name}, {fields}" )
        # Return something that looks like your mock "schema" object
        MockSchema = type("MockSchema", (), {"fields": fields})
        return MockSchema()

    def get_data_query(
        self,
        table_name,
        offset=0,
        limit=10,
        sort_column=None,
        sort_order=None,
        group_by=None,
        aggregate_columns=None,
        filter_model=None,
    ):
        """
        Run a SQL query against the given table, with optional:
          - offset/limit
          - sorting (sort_column and sort_order)
          - GROUP BY (group_by)
          - aggregates (aggregate_columns)
        
        Returns a pandas DataFrame.
        
        For example, aggregate_columns could be a list of dicts like:
           [{"column": "colB", "agg": "SUM"}, {"column": "colA", "agg": "COUNT"}]
        and group_by could be a list of columns, or a single column name.
        """
        # Reflect the table
        table = self._internal_schema(table_name)
        print(f"Bricks Input {sort_column}, {sort_order}, {group_by}, {aggregate_columns}, {filter_model}" )

        # If you need group_by or aggregates, build them:
        if group_by:
            # handle if group_by is a single str vs list
            if isinstance(group_by, str):
                group_by = [group_by]

            group_by_cols = [table.c[col] for col in group_by]
        else:
            group_by_cols = []

        # Build aggregate expressions (optional)
        agg_exprs = []
        if aggregate_columns:
            # e.g. aggregate_columns might look like: [{"column": "colB", "agg": "SUM"}]
            for agg in aggregate_columns:
                col_name = agg["column"]
                agg_func = agg["agg"].upper()
                col_obj = table.c[col_name]
                if agg_func == "SUM":
                    agg_exprs.append(func.sum(col_obj).label(f"{agg_func}_{col_name}"))
                elif agg_func == "AVG":
                    agg_exprs.append(func.avg(col_obj).label(f"{agg_func}_{col_name}"))
                elif agg_func == "COUNT":
                    agg_exprs.append(func.count(col_obj).label(f"{agg_func}_{col_name}"))
                elif agg_func == "MAX":
                    agg_exprs.append(func.max(col_obj).label(f"{agg_func}_{col_name}"))
                elif agg_func == "MIN":
                    agg_exprs.append(func.min(col_obj).label(f"{agg_func}_{col_name}"))
                # ... add more as needed
        else:
            # If no aggregate columns are specified, then just select all
            agg_exprs = [table.c[c.name] for c in table.columns]

         #If grouping, select group columns plus aggregates, then .group_by(...).
        if group_by_cols:
            stmt = select(*group_by_cols, *agg_exprs).group_by(*group_by_cols)
        else:
            stmt = select(*agg_exprs)

        # Apply filters (if any)
        if filter_model:
            print(f"Bricks filter {filter_model}")
            filter_conditions = []
            for column_name, condition in filter_model.items():
                if column_name not in table.c:
                    raise ValueError(f"Column '{column_name}' does not exist in table '{table_name}'")

                column = table.c[column_name]
                filter_value = condition.get("filter")
                filter_type = condition.get("filterType")
                filter_mode = condition.get("type", "contains")  # Default to "contains"

                if filter_type == "text":
                    if filter_mode == "contains":
                        filter_conditions.append(column.ilike(f"%{filter_value}%"))
                    elif filter_mode == "equals":
                        filter_conditions.append(column == filter_value)
                    elif filter_mode == "startsWith":
                        filter_conditions.append(column.ilike(f"{filter_value}%"))
                    elif filter_mode == "endsWith":
                        filter_conditions.append(column.ilike(f"%{filter_value}"))
                elif filter_type == "number":
                    if filter_mode == "equals":
                        filter_conditions.append(column == filter_value)
                    elif filter_mode == "greaterThan":
                        filter_conditions.append(column > filter_value)
                    elif filter_mode == "lessThan":
                        filter_conditions.append(column < filter_value)

            # Add the filters to the statement
            if filter_conditions:
                stmt = stmt.where(and_(*filter_conditions))

        # Sorting
        if sort_column and hasattr(table.c, sort_column):
            sort_col = table.c[sort_column]
            if sort_order and sort_order.lower() == "desc":
                stmt = stmt.order_by(desc(sort_col))
            else:
                stmt = stmt.order_by(asc(sort_col))

        # Offset/Limit
        stmt = stmt.offset(offset).limit(limit)

        print(f"Bricks stmt {str(stmt)}")
        # Execute
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df

    def check_duplicates(self, table_name):
        """
        1) Find duplicate transaction_ids.
        Return rows where transaction_id appears more than once.
        """
        print(f"Checking duplicates in {table_name}")
        duplicates_sql = text(f"""
            SELECT
                transaction_id,
                COUNT(*) AS duplicate_count
            FROM {table_name}
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
            limit 100
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(duplicates_sql)
            df = pd.DataFrame(rows.fetchall(), columns=rows.keys())
        
        return df

    def check_negative_debits_credits(self, table_name):
        """
        2) Find rows where debit or credit is negative.
        """
        print(f"Checking negative debits/credits in {table_name}")
        negative_sql = text(f"""
            SELECT *
            FROM {table_name}
            WHERE debit = 0
               OR credit = 0
            limit 100
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(negative_sql)
            df = pd.DataFrame(rows.fetchall(), columns=rows.keys())
        
        return df

    def check_region_country_mismatch(self, table_name):
        """
        3) Validate region/country pairs. 
           We'll do a simple inline check:
               - 'EU'  -> {Germany, France, Spain}
               - 'APAC'-> {India, China, Japan}
               - 'AMER'-> {USA, Canada, Mexico}
               - 'MEA' -> {South Africa, Egypt, UAE}
           
           Returns any rows that violate these mappings.
           
           (You can store valid mappings in a reference table
            and do a JOIN instead of a big WHERE.)
        """
        print(f"Checking region/country mismatch in {table_name}")
        mismatch_sql = text(f"""
            SELECT *
            FROM {table_name}
            WHERE (region = 'EU'   AND country NOT IN ('Germany','France','Spain'))
               OR (region = 'APAC' AND country NOT IN ('India','China','Japan'))
               OR (region = 'AMER' AND country NOT IN ('USA','Canada','Mexico'))
               OR (region = 'MEA'  AND country NOT IN ('South Africa','Egypt','UAE'))
            limit 100
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(mismatch_sql)
            df = pd.DataFrame(rows.fetchall(), columns=rows.keys())
        return df

    def save_row_data(self, table_name, changes):
        """
        Save updated row data to the database using SQLAlchemy's engine.
        :param changes: List of dictionaries representing changes from AgGrid.
        """
        try:
            for change in changes:
                row_data = change["data"]
                col_id = change["colId"]
                new_value = change["value"]

                # Assuming 'transaction_id' is the unique identifier
                transaction_id = row_data["transaction_id"]

                # Get the table object
                table = self.metadata.tables[table_name]

                # Build the update query
                query = table.update().where(
                    table.c.transaction_id == transaction_id
                ).values({col_id: new_value})

                # Execute the query
                with self.engine.connect() as conn:
                    conn.execute(query)
                
                print(f"Updated transaction_id={transaction_id}: Set {col_id}={new_value}")

        except Exception as e:
            print(f"Error saving data: {e}")