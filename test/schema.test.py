import unittest
from copy import deepcopy

from setup_tests import setup_tests

setup_tests()

from relationalize.schema import Schema

CASE_1 = {"1": 1, "2": "foobar", "3": False, "4": 1.2}

CASE_2 = {"1": "foobar", "2": 9.9, "3": True, "4": 9.5}

CASE_3 = {"1": None}
CASE_4 = {"1": 1}
CASE_5 = {"1": "foobar"}

CASE_1_DDL = """
CREATE TABLE "public"."test" (
    "1" BIGINT
    , "2" VARCHAR(65535)
    , "3" BOOLEAN
    , "4" FLOAT
);
""".strip()

CASE_2_DDL = """
CREATE TABLE "public"."test" (
    "1_int" BIGINT
    , "1_str" VARCHAR(65535)
    , "2_float" FLOAT
    , "2_str" VARCHAR(65535)
    , "3" BOOLEAN
    , "4" FLOAT
);
""".strip()


class SchemaTest(unittest.TestCase):
    def test_all_types_no_choice(self):
        schema = Schema()
        schema.read_object(CASE_1)
        self.assertDictEqual(
            {"1": "int", "2": "str", "3": "bool", "4": "float"}, schema.schema
        )

    def test_basic_choice(self):
        schema = Schema()
        schema.read_object(CASE_1)
        schema.read_object(CASE_2)
        self.assertDictEqual(
            {"1": "c-int-str", "2": "c-float-str", "3": "bool", "4": "float"},
            schema.schema,
        )

    def test_merge_noop(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        schema2 = Schema()
        schema2.read_object(CASE_1)

        schema3 = Schema()
        schema3.read_object(CASE_1)

        merged_schema = Schema.merge(schema1.schema, schema2.schema, schema3.schema)
        self.assertDictEqual(merged_schema.schema, schema1.schema)
        self.assertDictEqual(merged_schema.schema, schema2.schema)
        self.assertDictEqual(merged_schema.schema, schema3.schema)

    def test_merge_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        schema2 = Schema()
        schema2.read_object(CASE_2)

        merged_schema = Schema.merge(schema1.schema, schema2.schema)

        self.assertDictEqual(
            {"1": "c-int-str", "2": "c-float-str", "3": "bool", "4": "float"},
            merged_schema.schema,
        )

    def test_merge_equal_parse(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        schema2 = Schema()
        schema2.read_object(CASE_2)

        merged_schema = Schema.merge(schema1.schema, schema2.schema)

        schema3 = Schema()
        schema3.read_object(CASE_1)
        schema3.read_object(CASE_2)

        self.assertDictEqual(merged_schema.schema, schema3.schema)

    def test_convert_object_no_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)

        converted1 = schema1.convert_object(deepcopy(CASE_1))
        self.assertDictEqual(converted1, CASE_1)

    def test_convert_object_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        schema1.read_object(CASE_2)

        converted1 = schema1.convert_object(deepcopy(CASE_1))
        self.assertDictEqual(
            {"1_int": 1, "2_str": "foobar", "3": False, "4": 1.2}, converted1
        )
        converted2 = schema1.convert_object(deepcopy(CASE_2))
        self.assertDictEqual(
            {"1_str": "foobar", "2_float": 9.9, "3": True, "4": 9.5}, converted2
        )

    def test_generate_ddl_no_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        self.assertEqual(CASE_1_DDL, schema1.generate_ddl("test"))

    def test_generate_ddl_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        schema1.read_object(CASE_2)

        self.assertEqual(CASE_2_DDL, schema1.generate_ddl("test"))

    def test_none_cases(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        self.assertDictEqual({"1": "none"}, schema1.schema)

        schema1.read_object(CASE_4)
        self.assertDictEqual({"1": "int"}, schema1.schema)

        schema1.read_object(CASE_5)
        self.assertDictEqual({"1": "c-int-str"}, schema1.schema)

        schema1.read_object(CASE_3)
        self.assertDictEqual({"1": "c-int-str"}, schema1.schema)

    def test_none_convert(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)

        self.assertDictEqual({"1": None}, schema1.convert_object(CASE_3))

    def test_none_int_convert(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        schema1.read_object(CASE_4)

        self.assertDictEqual({"1": None}, schema1.convert_object(CASE_3))
        self.assertDictEqual({"1": 1}, schema1.convert_object(CASE_4))

    def test_none_choice_convert(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        schema1.read_object(CASE_4)
        schema1.read_object(CASE_5)

        self.assertDictEqual({"1": None}, schema1.convert_object(CASE_3))
        self.assertDictEqual({"1_int": 1}, schema1.convert_object(CASE_4))
        self.assertDictEqual({"1_str": "foobar"}, schema1.convert_object(CASE_5))

    def test_drop_null_columns(self):
        schema1 = Schema()
        schema1.read_object(CASE_3)
        self.assertDictEqual({"1": "none"}, schema1.schema)

        schema1.drop_null_columns()
        self.assertDictEqual({}, schema1.schema)

        schema2 = Schema()
        schema2.read_object(CASE_3)
        schema2.read_object(CASE_4)
        schema2.drop_null_columns()
        self.assertDictEqual({"1": "int"}, schema2.schema)

    def test_generate_output_columns_no_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        self.assertListEqual(["1", "2", "3", "4"], schema1.generate_output_columns())

    def test_generate_output_columns_choice(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        schema1.read_object(CASE_2)
        self.assertListEqual(
            ["1_int", "1_str", "2_float", "2_str", "3", "4"],
            schema1.generate_output_columns(),
        )

    def test_drop_special_char_columns(self):
        schema1 = Schema()
        schema1.read_object({"abc ": 1, "def@#": 1, "$$ghi": 1, "jkl": 1, "!@#mno": 1})
        print(schema1.schema)
        self.assertEqual(3, schema1.drop_special_char_columns())
        self.assertEqual(schema1.schema, {"abc ": 1, "jkl": 1})

    def test_drop_duplicate_columns1(self):
        schema1 = Schema()
        schema1.read_object(CASE_1)
        schema1.read_object(CASE_1)
        print(schema1.schema)
        self.assertEqual(4, schema1.drop_duplicate_columns())

    def test_drop_duplicate_columns2(self):
        schema1 = Schema()
        schema1.read_object(
            {"ABc ": 1, "DEf ": 1, "ghi": 1, "jkl": 1, "ABC": 1, "abc ": 1, "JkL": 1}
        )
        self.assertEqual(2, schema1.drop_duplicate_columns())
        self.assertEqual(
            schema1.schema,
            {"ABc ": 1, "DEf ": 1, "ghi": 1, "jkl": 1, "ABC": 1},
        )


if __name__ == "__main__":
    unittest.main()
