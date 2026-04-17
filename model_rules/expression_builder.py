from pyspark.sql import functions as spark_functions
from pyspark.sql.column import Column


class ColumnExpressionBuilder:
    def build_expression_from_rule_branch(
        self,
        rule_definition: dict,
        branch_definition: dict
    ) -> Column:
        """
        Constrói a expressão (then ou otherwise)
        """

        if "literal" in branch_definition:
            return spark_functions.lit(branch_definition["literal"])

        input_column_name = branch_definition.get(
            "input",
            rule_definition["input"]
        )

        current_expression = spark_functions.col(input_column_name)

        transformation_pipeline = branch_definition.get("pipeline", [])

        for transformation_step in transformation_pipeline:
            current_expression = self._apply_transformation_step(
                current_expression,
                transformation_step
            )

        return current_expression

    def _apply_transformation_step(
        self,
        expression: Column,
        transformation_step: dict
    ) -> Column:

        operation_type = transformation_step["op"]

        if operation_type == "cast":
            return expression.cast(transformation_step["type"])

        if operation_type == "lpad":
            return spark_functions.lpad(
                expression,
                transformation_step["len"],
                transformation_step["fill"]
            )

        if operation_type == "rpad":
            return spark_functions.rpad(
                expression,
                transformation_step["len"],
                transformation_step["fill"]
            )

        if operation_type == "substring":
            return spark_functions.substring(
                expression,
                transformation_step["start"],
                transformation_step["len"]
            )

        if operation_type == "trim":
            return spark_functions.trim(expression)

        if operation_type == "upper":
            return spark_functions.upper(expression)

        if operation_type == "lower":
            return spark_functions.lower(expression)

        raise ValueError(f"Transformação não suportada: {operation_type}")