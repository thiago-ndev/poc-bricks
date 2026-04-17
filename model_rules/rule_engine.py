from pyspark.sql import DataFrame, functions as spark_functions

from build_condition_expression import ConditionExpressionBuilder
from expression_builder import ColumnExpressionBuilder


class DataTransformationEngine:
    def __init__(self, configuration: dict):
        self.configuration = configuration
        self.condition_builder = ConditionExpressionBuilder()
        self.expression_builder = ColumnExpressionBuilder()

    def apply_all_rules_to_dataframe(self, input_dataframe: DataFrame):
        """
        Aplica todas as regras do JSON no DataFrame
        """

        ordered_column_rules = self._get_column_rules_sorted_by_order()

        list_of_new_column_expressions = []

        for column_rule in ordered_column_rules:
            column_expression = self._build_column_expression(column_rule)

            aliased_expression = column_expression.alias(
                column_rule["output"]
            )

            list_of_new_column_expressions.append(aliased_expression)

        output_only = self.configuration.get("options", {}).get(
            "select_only_output_columns",
            False
        )

        if output_only:
            transformed_dataframe = input_dataframe.select(
                *list_of_new_column_expressions
            )
        else:
            transformed_dataframe = input_dataframe.select(
                "*",
                *list_of_new_column_expressions
            )

        ordered_output_columns = self._get_ordered_output_column_names()

        return transformed_dataframe, ordered_output_columns

    def _get_column_rules_sorted_by_order(self):
        return sorted(
            self.configuration["columns"],
            key=lambda rule: rule["order"]
        )

    def _get_ordered_output_column_names(self):
        return [
            rule["output"]
            for rule in self._get_column_rules_sorted_by_order()
        ]

    def _build_column_expression(self, column_rule: dict):
        """
        Monta a expressão final da coluna
        """

        then_expression = self.expression_builder.build_expression_from_rule_branch(
            column_rule,
            column_rule["then"]
        )

        if "condition" not in column_rule:
            return then_expression

        condition_expression = self.condition_builder.build_condition_expression(
            column_rule["condition"]
        )

        otherwise_definition = column_rule.get("otherwise")

        if otherwise_definition:
            otherwise_expression = self.expression_builder.build_expression_from_rule_branch(
                column_rule,
                otherwise_definition
            )
        else:
            otherwise_expression = spark_functions.col(column_rule["input"])

        return spark_functions.when(
            condition_expression,
            then_expression
        ).otherwise(otherwise_expression)