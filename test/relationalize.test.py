import json
import unittest

from setup_tests import setup_tests

setup_tests()

from relationalize import Relationalize
from relationalize.utils import create_local_buffer

CASE_1 = {"1": 1, "2": "foobar", "3": False, "4": 1.2}

CASE_2 = {"1": "foobar", "2": 9.9, "3": True, "4": 9.5}

CASE_3 = {"1": [1, 2], "2": "foobar"}

CASE_4 = {"1": [{"2": "foobar", "3": 1}, {"2": "barfoo", "3": 3}], "2": "foobar"}

CASE_5 = {"1": [[1], [2, 3]]}

CASE_6 = {
    "1": [{"2": "foobar", "3": [1, 2]}, {"2": "barfoo", "3": [3, 4]}],
    "2": "foobar",
}

CASE_7 = {"1": {"2": 1, "3": "foobar"}}

CASE_8 = {"1": [[{"2": 3}, {"2": 4}], [{"2": 5}, {"2": 6}]]}


class RelationalizeTest(unittest.TestCase):
    def test_no_array(self):
        with Relationalize("test_case_1", create_local_buffer()) as r:
            r.relationalize([CASE_1])
            self.assertListEqual(["test_case_1"], list(r.outputs.keys()))
            r.outputs["test_case_1"].seek(0)
            self.assertEqual(
                f"{json.dumps(CASE_1)}\n",
                r.outputs["test_case_1"].read(),
            )

    def test_two_records_no_array(self):
        with Relationalize("test_case_2", create_local_buffer()) as r:
            r.relationalize([CASE_1, CASE_2])
            self.assertListEqual(["test_case_2"], list(r.outputs.keys()))
            r.outputs["test_case_2"].seek(0)
            self.assertEqual(
                f"{json.dumps(CASE_1)}\n{json.dumps(CASE_2)}\n",
                r.outputs["test_case_2"].read(),
            )

    def test_literal_array(self):
        with Relationalize("test_case_3", create_local_buffer()) as r:
            r.relationalize([CASE_3])
            self.assertListEqual(
                sorted(["test_case_3", "test_case_3_1"]), sorted(list(r.outputs.keys()))
            )
            r.outputs["test_case_3"].seek(0)
            r.outputs["test_case_3_1"].seek(0)
            self.assertRegex(
                r.outputs["test_case_3"].read(),
                r"{\"1\": \"R_[a-z0-9]{32}\", \"2\": \"foobar\"}",
            )
            file_lines = r.outputs["test_case_3_1"].read().strip().split("\n")

            self.assertRegex(
                file_lines[0],
                r"{\"1__val_\": 1, \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 0}",
            )
            self.assertRegex(
                file_lines[1],
                r"{\"1__val_\": 2, \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 1}",
            )

    def test_struct_array(self):
        with Relationalize("test_case_4", create_local_buffer()) as r:
            r.relationalize([CASE_4])
            self.assertListEqual(
                sorted(["test_case_4", "test_case_4_1"]), sorted(list(r.outputs.keys()))
            )

            r.outputs["test_case_4"].seek(0)
            r.outputs["test_case_4_1"].seek(0)

            test_case_4_content = r.outputs["test_case_4"].read()
            file_lines = r.outputs["test_case_4_1"].read().strip().split("\n")

        self.assertRegex(
            test_case_4_content,
            r"{\"1\": \"R_[a-z0-9]{32}\", \"2\": \"foobar\"}",
        )
        self.assertRegex(
            file_lines[0],
            r"{\"1_2\": \"foobar\", \"1_3\": 1, \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 0}",
        )
        self.assertRegex(
            file_lines[1],
            r"{\"1_2\": \"barfoo\", \"1_3\": 3, \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 1}",
        )

        self.assertEqual(
            json.loads(test_case_4_content)["1"],
            json.loads(file_lines[0])["1__rid_"],
        )

        self.assertEqual(
            json.loads(test_case_4_content)["1"],
            json.loads(file_lines[1])["1__rid_"],
        )

    def test_list_list_literal(self):
        with Relationalize("test_case_5", create_local_buffer()) as r:
            r.relationalize([CASE_5])

            self.assertListEqual(
                sorted(["test_case_5", "test_case_5_1", "test_case_5_1__val_"]),
                sorted(list(r.outputs.keys())),
            )
            r.outputs["test_case_5"].seek(0)
            r.outputs["test_case_5_1"].seek(0)
            r.outputs["test_case_5_1__val_"].seek(0)

            test_case_5_content = r.outputs["test_case_5"].read()

            test_case_5_1_content = r.outputs["test_case_5_1"].read()
            test_case_5_1_list = test_case_5_1_content.strip().split("\n")

            self.assertEqual(len(test_case_5_1_list), 2)

            test_case_5_1__val_content = r.outputs["test_case_5_1__val_"].read()
            test_case_5_1__val_list = test_case_5_1__val_content.strip().split("\n")

            self.assertEqual(len(test_case_5_1__val_list), 3)

            self.assertRegex(test_case_5_content, r"{\"1\": \"R_[a-z0-9]{32}\"}")

            self.assertRegex(
                test_case_5_1_list[0],
                r"{\"1__val_\": \"R_[a-z0-9]{32}\", \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 0}",
            )
            self.assertRegex(
                test_case_5_1_list[1],
                r"{\"1__val_\": \"R_[a-z0-9]{32}\", \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 1}",
            )

            self.assertRegex(
                test_case_5_1__val_list[0],
                r"{\"1__val___val_\": 1, \"1__val___rid_\": \"R_[a-z0-9]{32}\", \"1__val___index_\": 0}",
            )

            self.assertRegex(
                test_case_5_1__val_list[1],
                r"{\"1__val___val_\": 2, \"1__val___rid_\": \"R_[a-z0-9]{32}\", \"1__val___index_\": 0}",
            )

            self.assertRegex(
                test_case_5_1__val_list[2],
                r"{\"1__val___val_\": 3, \"1__val___rid_\": \"R_[a-z0-9]{32}\", \"1__val___index_\": 1}",
            )

            self.assertEqual(
                json.loads(test_case_5_content)["1"],
                json.loads(test_case_5_1_list[0])["1__rid_"],
            )

            self.assertEqual(
                json.loads(test_case_5_content)["1"],
                json.loads(test_case_5_1_list[1])["1__rid_"],
            )

            self.assertEqual(
                json.loads(test_case_5_1_list[0])["1__val_"],
                json.loads(test_case_5_1__val_list[0])["1__val___rid_"],
            )

            self.assertEqual(
                json.loads(test_case_5_1_list[1])["1__val_"],
                json.loads(test_case_5_1__val_list[1])["1__val___rid_"],
            )

            self.assertEqual(
                json.loads(test_case_5_1_list[1])["1__val_"],
                json.loads(test_case_5_1__val_list[2])["1__val___rid_"],
            )

    def test_nested_array_struct_array(self):
        with Relationalize("test_case_6", create_local_buffer()) as r:
            r.relationalize([CASE_6])
            self.assertListEqual(
                sorted(["test_case_6", "test_case_6_1", "test_case_6_1_3"]),
                sorted(list(r.outputs.keys())),
            )
            r.outputs["test_case_6"].seek(0)
            r.outputs["test_case_6_1"].seek(0)
            r.outputs["test_case_6_1_3"].seek(0)

            test_case_6_content = r.outputs["test_case_6"].read()

            self.assertRegex(
                test_case_6_content,
                r"{\"1\": \"R_[a-z0-9]{32}\", \"2\": \"foobar\"}",
            )

            file_lines = r.outputs["test_case_6_1"].read().strip().split("\n")

            self.assertRegex(
                file_lines[0],
                r"{\"1_2\": \"foobar\", \"1_3\": \"R_[a-z0-9]{32}\", \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 0}",
            )
            self.assertRegex(
                file_lines[1],
                r"{\"1_2\": \"barfoo\", \"1_3\": \"R_[a-z0-9]{32}\", \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 1}",
            )

            sub_file_lines = r.outputs["test_case_6_1_3"].read().strip().split("\n")

            self.assertRegex(
                sub_file_lines[0],
                r"{\"1_3__val_\": 1, \"1_3__rid_\": \"R_[a-z0-9]{32}\", \"1_3__index_\": 0}",
            )

            self.assertRegex(
                sub_file_lines[1],
                r"{\"1_3__val_\": 2, \"1_3__rid_\": \"R_[a-z0-9]{32}\", \"1_3__index_\": 1}",
            )

            self.assertRegex(
                sub_file_lines[2],
                r"{\"1_3__val_\": 3, \"1_3__rid_\": \"R_[a-z0-9]{32}\", \"1_3__index_\": 0}",
            )

            self.assertRegex(
                sub_file_lines[3],
                r"{\"1_3__val_\": 4, \"1_3__rid_\": \"R_[a-z0-9]{32}\", \"1_3__index_\": 1}",
            )

            self.assertEqual(
                json.loads(test_case_6_content)["1"],
                json.loads(file_lines[0])["1__rid_"],
            )
            self.assertEqual(
                json.loads(test_case_6_content)["1"],
                json.loads(file_lines[1])["1__rid_"],
            )

            self.assertEqual(
                json.loads(file_lines[0])["1_3"],
                json.loads(sub_file_lines[0])["1_3__rid_"],
            )
            self.assertEqual(
                json.loads(file_lines[0])["1_3"],
                json.loads(sub_file_lines[1])["1_3__rid_"],
            )

            self.assertEqual(
                json.loads(file_lines[1])["1_3"],
                json.loads(sub_file_lines[2])["1_3__rid_"],
            )
            self.assertEqual(
                json.loads(file_lines[1])["1_3"],
                json.loads(sub_file_lines[3])["1_3__rid_"],
            )

    def test_flatten_struct(self):
        with Relationalize("test_case_7", create_local_buffer()) as r:
            r.relationalize([CASE_7])
            self.assertListEqual(
                sorted(["test_case_7"]), sorted(list(r.outputs.keys()))
            )
            r.outputs["test_case_7"].seek(0)
            self.assertDictEqual(
                {"1_2": 1, "1_3": "foobar"},
                json.loads(r.outputs["test_case_7"].read()),
            )

    def test_list_list_struct(self):
        with Relationalize("test_case_8", create_local_buffer()) as r:
            r.relationalize([CASE_8])

            self.assertListEqual(
                sorted(["test_case_8", "test_case_8_1", "test_case_8_1__val_"]),
                sorted(list(r.outputs.keys())),
            )
            r.outputs["test_case_8"].seek(0)
            r.outputs["test_case_8_1"].seek(0)
            r.outputs["test_case_8_1__val_"].seek(0)

            test_case_8_content = r.outputs["test_case_8"].read()

            test_case_8_1_content = r.outputs["test_case_8_1"].read()
            test_case_8_1_list = test_case_8_1_content.strip().split("\n")

            self.assertEqual(len(test_case_8_1_list), 2)

            test_case_8_1__val_content = r.outputs["test_case_8_1__val_"].read()
            test_case_8_1__val_list = test_case_8_1__val_content.strip().split("\n")

            self.assertEqual(len(test_case_8_1__val_list), 4)

            self.assertRegex(test_case_8_content, r"{\"1\": \"R_[a-z0-9]{32}\"}")

            self.assertRegex(
                test_case_8_1_list[0],
                r"{\"1__val_\": \"R_[a-z0-9]{32}\", \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 0}",
            )
            self.assertRegex(
                test_case_8_1_list[1],
                r"{\"1__val_\": \"R_[a-z0-9]{32}\", \"1__rid_\": \"R_[a-z0-9]{32}\", \"1__index_\": 1}",
            )

            self.assertRegex(
                test_case_8_1__val_list[0],
                r"{\"1__val__2\": 3, \"1__val___rid_\": \"R_[a-z0-9]{32}\", \"1__val___index_\": 0}",
            )
            self.assertRegex(
                test_case_8_1__val_list[1],
                r"{\"1__val__2\": 4, \"1__val___rid_\": \"R_[a-z0-9]{32}\", \"1__val___index_\": 1}",
            )
            self.assertRegex(
                test_case_8_1__val_list[2],
                r"{\"1__val__2\": 5, \"1__val___rid_\": \"R_[a-z0-9]{32}\", \"1__val___index_\": 0}",
            )
            self.assertRegex(
                test_case_8_1__val_list[3],
                r"{\"1__val__2\": 6, \"1__val___rid_\": \"R_[a-z0-9]{32}\", \"1__val___index_\": 1}",
            )

            self.assertEqual(
                json.loads(test_case_8_content)["1"],
                json.loads(test_case_8_1_list[0])["1__rid_"],
            )

            self.assertEqual(
                json.loads(test_case_8_content)["1"],
                json.loads(test_case_8_1_list[1])["1__rid_"],
            )

            self.assertEqual(
                json.loads(test_case_8_1_list[0])["1__val_"],
                json.loads(test_case_8_1__val_list[0])["1__val___rid_"],
            )

            self.assertEqual(
                json.loads(test_case_8_1_list[0])["1__val_"],
                json.loads(test_case_8_1__val_list[1])["1__val___rid_"],
            )

            self.assertEqual(
                json.loads(test_case_8_1_list[1])["1__val_"],
                json.loads(test_case_8_1__val_list[2])["1__val___rid_"],
            )

            self.assertEqual(
                json.loads(test_case_8_1_list[1])["1__val_"],
                json.loads(test_case_8_1__val_list[3])["1__val___rid_"],
            )

            self.assertEqual(json.loads(test_case_8_1_list[0])["1__index_"], 0)
            self.assertEqual(json.loads(test_case_8_1_list[1])["1__index_"], 1)

            self.assertEqual(
                json.loads(test_case_8_1__val_list[0])["1__val___index_"], 0
            )
            self.assertEqual(
                json.loads(test_case_8_1__val_list[1])["1__val___index_"], 1
            )
            self.assertEqual(
                json.loads(test_case_8_1__val_list[2])["1__val___index_"], 0
            )
            self.assertEqual(
                json.loads(test_case_8_1__val_list[3])["1__val___index_"], 1
            )


if __name__ == "__main__":
    unittest.main()
