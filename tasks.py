"""
1. делаются запросы к апи в потоках - IO
2. результат выполнения кладётся в очередь
3. берутся данные из очереди и запускается анализатор - CPU
4. результаты анализа сохраняются в файлы (не на нашей стороне)
5. после выполнения анализа, кладутся данные в очередь - название файла с результатом
6. берутся данные из очереди и запускается агрегатор - запись в общий файл - IO
7. когда все данные запишутся в агрегатор, то запустить анализатор - CPU, но как параллелить?
8. в результате работы анализатора, записать ещё поле рейтинг в файл - IO
9. написать какой город лучший - IO
"""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue
import subprocess
from threading import Thread

from external.client import YandexWeatherAPI
from utils import CITIES

format = '%(asctime)s: %(message)s'
logging.basicConfig(format=format, level=logging.INFO, datefmt='%H:%M:%S')
result_folder = 'result/'
result_file_name_appendix = '_response'

HOURS_COUNT_NAME = 'hours_count'
AVERAGE_TEMPERATURE_NAME = 'temp_avg'
RELEVANT_CONDITION_COUNT_NAME = 'relevant_cond_hours'


class DataFetchingTask:
    weather_api = YandexWeatherAPI()

    def __init__(self, analyse_queue: Queue):
        self.analyse_queue = analyse_queue

    def get_weather(self, city_data: tuple) -> dict | None:
        city = city_data[0]
        url = city_data[1]
        logging.info('Получаем погоду для %s', city)
        try:
            result = self.weather_api.get_forecasting(url)
            logging.info('Получили погоду для %s', city)
        except Exception:
            logging.info('Не получилось получить погоду для %s', city)
            return None
        self.save_data_to_file({city: result})
        return {city: result}

    def run(self, cities: dict) -> None:
        logging.info('Создали пул для получения погоды...')

        with ThreadPoolExecutor() as pool:
            pool.map(self.get_weather, cities.items())

    def save_data_to_file(self, data):
        if not data:
            return
        city = list(data.keys())[0]
        logging.info('Сохраняем полученные по апи результаты для %s', city)
        filename = result_folder + city + result_file_name_appendix
        with open(filename, 'w') as f:
            json.dump(list(data.values())[0], f)
        self.analyse_queue.put(filename)


class DataCalculationTask:
    python_file_path = 'external/analyzer.py'

    def __init__(self, analyse_queue: Queue, result_queue: Queue):
        self.analyse_queue = analyse_queue
        self.result_queue = result_queue

    @staticmethod
    def get_args(input_value, output_value):
        return ['--input', input_value, '--output', output_value, '--verbose']

    def get_data(self, input_data_file_name):
        logging.info('Запускаем анализатор для %s', input_data_file_name)
        analysed_file_name = input_data_file_name + '_analysed'
        subprocess.run(
            [
                'python3',
                self.python_file_path,
            ] + self.get_args(input_data_file_name, analysed_file_name)
        )
        self.result_queue.put(analysed_file_name)

    def run(self):
        while item := self.analyse_queue.get():
            self.get_data(item)


class DataAggregationTask:
    result_file = 'result.json'

    def __init__(self, result_queue: Queue):
        self.result_queue = result_queue
        with open(self.result_file, 'w'):
            pass

    def run(self):
        logging.info('Начинаем объединение вычисленных данных')
        res = []
        while item := self.result_queue.get():
            json_data = self.get_json(item)
            if json_data:
                res.append(json_data)
        logging.info('Очередь с результатами пуста')
        t = Thread(target=self.write_data, args=([item for item in res if item],))
        t.start()
        t.join()

    def get_avg_data(self, item):
        temp = []
        cond = []

        for day in item:
            if day[HOURS_COUNT_NAME] != 11:
                continue
            temp.append(day[AVERAGE_TEMPERATURE_NAME])
            cond.append(day[RELEVANT_CONDITION_COUNT_NAME])
        return {
            'avg_temp': sum(temp) / len(temp),
            'avg_cond': sum(cond) / len(cond),
        }

    def get_json(self, item):
        with open(item, 'r') as f:
            data = json.load(f)
        if not data:
            return
        days = data['days']
        result_json = {
            'city': item.split('/')[1].split('_')[0],
            'days': [],
        }
        for day in days:
            result_json['days'].append(
                {
                    'date': day['date'],
                    'temp_avg': day[AVERAGE_TEMPERATURE_NAME],
                    'relevant_cond_hours': day[RELEVANT_CONDITION_COUNT_NAME],
                }
            )
        result_json.update(self.get_avg_data(days))
        return result_json

    def write_data(self, json_data):
        with open(self.result_file, 'a') as f:
            json.dump(json_data, f, indent=2)

    def write_city_ratings(self, ratings: dict[str, int]):
        with open(self.result_file, 'r') as f:
            data = json.load(f)
            for item in data:
                item['rating'] = ratings[item['city']]
        with open(self.result_file, 'w') as f:
            json.dump(data, f, indent=2)


class DataAnalyzingTask:
    result_file = 'result.json'

    def run(self):
        logging.info('Анализируем данные')
        with open(self.result_file, 'r') as f:
            data = json.load(f)

        cities_with_rating = sorted(data, key=lambda city: (-city['avg_temp'], -city['avg_cond']))
        try:
            logging.info('Лучший город - %s', cities_with_rating[0]['city'])
        except IndexError:
            logging.error('Не получилось получить лучший город')

        for i, city in enumerate(cities_with_rating):
            print(f'{i + 1} - {city["city"]} - avg_temp: {city["avg_temp"]} - cond: {city["avg_cond"]}')
        return {city['city']: i + 1 for i, city in enumerate(cities_with_rating)}


if __name__ == '__main__':
    start = time.time()

    analyse_queue: Queue = Queue()
    result_queue: Queue = Queue()

    data_fetching = DataFetchingTask(analyse_queue=analyse_queue)
    data_calculation = DataCalculationTask(analyse_queue=analyse_queue, result_queue=result_queue)
    data_aggregation = DataAggregationTask(result_queue=result_queue)

    p1 = Process(target=data_fetching.run, args=(CITIES,))
    p2 = Process(target=data_calculation.run)
    p3 = Process(target=data_aggregation.run)

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    analyse_queue.put(None)
    p2.join()
    result_queue.put(None)
    p3.join()

    rating = DataAnalyzingTask().run()
    data_aggregation.write_city_ratings(rating)

    end = time.time()

    logging.info('Затраченное время - %s', end - start)
