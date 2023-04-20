import requests
import json
from abc import ABC, abstractmethod
from pg_utils.utils import *


class Engine(ABC):
    """Класс - абстракция"""
    def __init__(self):
        super().__init__()

    @abstractmethod
    def get_request(self):
        """Абстракция: get-запрос"""
        pass

class HH(Engine):
    """Класс работы с api HH"""
    objects_list = []

    @staticmethod
    def inp_vacancy_list(vacancies):
        """Приводит к нужному формату для api, для поиска по нескольким вводным"""
        return vacancies.replace(',', ' OR')

    @classmethod
    def clear_object_list(cls):
        """Очищает список объектов класса"""
        cls.objects_list.clear()

    def get_request(self, page_number=0, text='', company_list=[], search_area='vacancies'):
        """get-запрос к api HH"""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0'}
        par = {'per_page': '100', 'page': page_number, 'text': self.inp_vacancy_list(text), 'employer_id': company_list}
        response = requests.get(f'https://api.hh.ru/{search_area}', params=par, headers=headers).json()
        return response

    @staticmethod
    def append_areas():
        """Наполянем список класса всеми объектами локаций, предварительно очищая данный список"""
        HH.clear_object_list()
        areas = HH().get_request(search_area='areas')

        for area in areas:
            HH.objects_list.append(area)

    @staticmethod
    def append_vacancies(cicles_cnt, company_list=[], text=''):
        """
        Наполняет список класса HH заданными в параметрах объектами:
        cicles_cnt - количество циклов (страниц на HH по 100 записей);
        company_list - список id компаний по которым будет производиться поиск;
        text - ключевые слова поиска по вакансии. Можно несколько слов через запятую.
        """
        HH.clear_object_list()
        page_number = 0

        for cicles in range(cicles_cnt):
            hh_request = HH().get_request(page_number=page_number, text=text, company_list=company_list)['items']
            for i in hh_request:
                HH.objects_list.append(i)
            page_number += 1

    @staticmethod
    def append_dictionaries(dict_name):
        """Наполянем список класса всеми объектами валют, предварительно очищая данный список"""
        HH.clear_object_list()
        dictionaries = HH().get_request(search_area='dictionaries')

        for dictionary in dictionaries[dict_name]:
            HH.objects_list.append(dictionary)

    @staticmethod
    def append_employers(company_list):
        """Наполянем список класса информацией о компаниях из введенного в параметр списка id компаний"""
        HH.clear_object_list()
        for company in company_list:
            company_info = HH().get_request(search_area=f'employers/{company}')
            HH.objects_list.append(company_info)


