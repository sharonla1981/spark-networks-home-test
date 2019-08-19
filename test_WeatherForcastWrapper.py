from .weather_forcast_data_load import WeatherForcastWrapper
import unittest

class test_WeatherForcastWrapper(unittest.TestCase):
    #test get_weather_data_from_api()
    def test_get_weather_data_from_api_for_correct_return_data_type(self):
        # get the actual results from the function
        wf = WeatherForcastWrapper()
        result = wf.get_weather_data_from_api('London,uk','forecast')
        # expected
        expected_data_type = type({})
        self.assertEqual(expected_data_type,type(result))

    def test_get_weather_data_from_api_for_data_not_empty(self):
        # get the actual results from the function
        wf = WeatherForcastWrapper()
        result = wf.get_weather_data_from_api('London,uk', 'weather')
        # expected
        expected_count = 0
        self.assertGreater(len(result),expected_count)

#run tests
if __name__ == '__main__':
    unittest.main()