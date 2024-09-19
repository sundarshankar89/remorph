from sqlglot.dialects.teradata import Teradata as org_teradata
import typing as t

from sqlglot import exp
from sqlglot.tokens import TokenType


class Teradata(org_teradata):

    class Parser(org_teradata.Parser):

        STATEMENT_PARSERS = {
            **org_teradata.Parser.STATEMENT_PARSERS,
            TokenType.CREATE: lambda self: self._parse_create(),
            TokenType.WITH: lambda self: self._parse_with(),
        }

        CONSTRAINT_PARSERS = {
            **org_teradata.Parser.CONSTRAINT_PARSERS,
            "COMPRESS": lambda self: self._parse_compress(),
        }

        # PROPERTY_PARSERS = {
        #     "MAP": lambda self: self._parse_map_property(),
        # }

        def _parse_create(self) -> exp.Create | exp.Command:

            # Note: this can't be None because we've matched a statement parser
            start = self._prev
            comments = self._prev_comments

            replace = start.text.upper() == "REPLACE" or self._match_pair(TokenType.OR, TokenType.REPLACE)
            unique = self._match(TokenType.UNIQUE)

            if self._match_pair(TokenType.TABLE, TokenType.FUNCTION, advance=False):
                self._advance()

            properties = None
            create_token = self._match_set(self.CREATABLES) and self._prev

            if not create_token:
                # exp.Properties.Location.POST_CREATE
                properties = self._parse_properties()
                create_token = self._match_set(self.CREATABLES) and self._prev

                if not properties or not create_token:
                    return self._parse_as_command(start)

            exists = self._parse_exists(not_=True)
            this = None
            expression: t.Optional[exp.Expression] = None
            indexes = None
            no_schema_binding = None
            begin = None
            end = None
            clone = None

            def extend_props(temp_props: t.Optional[exp.Properties]) -> None:
                nonlocal properties
                if properties and temp_props:
                    properties.expressions.extend(temp_props.expressions)
                elif temp_props:
                    properties = temp_props

            if create_token.token_type in (TokenType.FUNCTION, TokenType.PROCEDURE):
                this = self._parse_user_defined_function(kind=create_token.token_type)

                # exp.Properties.Location.POST_SCHEMA ("schema" here is the UDF's type signature)
                extend_props(self._parse_properties())

                self._match(TokenType.ALIAS)

                if self._match(TokenType.COMMAND):
                    expression = self._parse_as_command(self._prev)
                else:
                    begin = self._match(TokenType.BEGIN)
                    return_ = self._match_text_seq("RETURN")

                    if self._match(TokenType.STRING, advance=False):
                        # Takes care of BigQuery's JavaScript UDF definitions that end in an OPTIONS property
                        # # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
                        expression = self._parse_string()
                        extend_props(self._parse_properties())
                    else:
                        expression = self._parse_statement()

                    end = self._match_text_seq("END")

                    if return_:
                        expression = self.expression(exp.Return, this=expression)
            elif create_token.token_type == TokenType.INDEX:
                this = self._parse_index(index=self._parse_id_var())
            elif create_token.token_type in self.DB_CREATABLES:
                table_parts = self._parse_table_parts(schema=True)

                # exp.Properties.Location.POST_NAME
                self._match(TokenType.COMMA)
                extend_props(self._parse_properties(before=True))

                this = self._parse_schema(this=table_parts)

                # exp.Properties.Location.POST_SCHEMA and POST_WITH
                extend_props(self._parse_properties())

                self._match(TokenType.ALIAS)
                if not self._match_set(self.DDL_SELECT_TOKENS, advance=False):
                    # exp.Properties.Location.POST_ALIAS
                    extend_props(self._parse_properties())

                expression = self._parse_ddl_select()

                if create_token.token_type == TokenType.TABLE:
                    # exp.Properties.Location.POST_EXPRESSION
                    extend_props(self._parse_properties())

                    indexes = []
                    while True:
                        index = self._parse_index()

                        # exp.Properties.Location.POST_INDEX
                        extend_props(self._parse_properties())

                        if not index:
                            break
                        else:
                            self._match(TokenType.COMMA)
                            indexes.append(index)
                elif create_token.token_type == TokenType.VIEW:
                    if self._match_text_seq("WITH", "NO", "SCHEMA", "BINDING"):
                        no_schema_binding = True

                shallow = self._match_text_seq("SHALLOW")

                if self._match_texts(self.CLONE_KEYWORDS):
                    copy = self._prev.text.lower() == "copy"
                    clone = self.expression(exp.Clone, this=self._parse_table(schema=True), shallow=shallow, copy=copy)
                if self._match(TokenType.WITH):
                    pass

            return self.expression(
                exp.Create,
                comments=comments,
                this=this,
                kind=create_token.text,
                replace=replace,
                unique=unique,
                expression=expression,
                exists=exists,
                properties=properties,
                indexes=indexes,
                no_schema_binding=no_schema_binding,
                begin=begin,
                end=end,
                clone=clone,
            )

        def _parse_types(
            self, check_func: bool = False, schema: bool = False, allow_identifiers: bool = True
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_types(check_func=check_func, schema=schema, allow_identifiers=allow_identifiers)

            if (
                isinstance(this, exp.DataType)
                and this.is_type("numeric", "decimal", "number", "integer", "int", "smallint", "bigint")
                and not this.expressions
            ):
                return exp.DataType.build("DECIMAL(38,0)")

            return this

        def _parse_compress(self) -> exp.CompressColumnConstraint:
            if self._match(TokenType.L_PAREN, advance=False):
                return self.expression(exp.CompressColumnConstraint, this=self._parse_wrapped_csv(self._parse_bitwise))
            bitwise_expr = self._parse_bitwise()
            this_expr = bitwise_expr if bitwise_expr else exp.Literal(this=" ", is_string=True)
            return self.expression(exp.CompressColumnConstraint, this=this_expr)
