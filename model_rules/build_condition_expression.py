from pyspark.sql import functions as spark_functions
from pyspark.sql.column import Column

class ConditionExpressionBuilder:
    def build_condition_expression(self, condition_definition: dict) -> Column:
        """
        Constrói uma expressão Spark baseada na condição do JSON.
        Suporta AND, OR e operadores simples.
        """

        operation_type = condition_definition["op"]

        if operation_type == "and":
            return self._build_and_condition(condition_definition["conditions"])

        if operation_type == "or":
            return self._build_or_condition(condition_definition["conditions"])

        return self._build_simple_condition(condition_definition)



    def _build_and_condition(self, list_of_conditions: list) -> Column:
        combined_condition = None

        for condition_item in list_of_conditions:
            current_condition = self.build_condition_expression(condition_item)

            if combined_condition is None:
                combined_condition = current_condition
            else:
                combined_condition = combined_condition & current_condition

        return combined_condition


    def _build_or_condition(self, list_of_conditions: list) -> Column:
        combined_condition = None

        for condition_item in list_of_conditions:
            current_condition = self.build_condition_expression(condition_item)
            if condition_item is None:
                combined_condition = current_condition

            else:
                combined_condition = combined_condition | current_condition

        return combined_condition

    @staticmethod
    def _build_simple_condition(self, condition_definition: dict) -> Column:
        column_name = condition_definition["column"]
        operator = condition_definition["operator"]
        comparison_value = condition_definition.get("value")

        column_expression = spark_functions.col(column_name)

        if "cast" in condition_definition:
            column_expression =  column_expression.cast(condition_definition["cast"])

        if operator == "==":
            return column_expression == comparison_value

        if operator == "!=":
            return column_expression != comparison_value

        if operator == ">":
            return column_expression > comparison_value

        if operator == "<":
            return column_expression < comparison_value

        if operator == "isNull":
            return column_expression.isNull()

        if operator == "isNotNull":
            return column_expression.isNotNull()

        raise ValueError(f"Operador não suportado: {operator}")











