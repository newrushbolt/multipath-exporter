import unittest
import multipath_exporter


class TestMultipathExporter(unittest.TestCase):

    cmd_123 = [
        'sh', '-c',
        'for number in 1 2 3; do sleep 1;echo $number;done'
    ]
    cmd_result_123 = "1\n2\n3\n"

    def test_cmd_timeout_expired(self):
        cmd_result = multipath_exporter.run_command_w_timeout(self.cmd_123, 2)
        self.assertEqual(cmd_result, None)

    def test_cmd_timeout(self):
        cmd_result = multipath_exporter.run_command_w_timeout(self.cmd_123, 4)
        self.assertEqual(cmd_result, self.cmd_result_123)


if __name__ == '__main__':
    unittest.main()
