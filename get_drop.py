# -*- coding: utf-8 -*-

import pandas as pd
import requests
import json
from time import sleep
import os
from datetime import datetime as dt
from datetime import timedelta
import re


class Drop:
    def __init__(self, data):
        self.data = data
        self.start()

    def select_file(self):
        list_file = []
        for file in os.listdir('.'):
            if '.csv' in file:
                print(file)
                list_file.append(file)

        while True:
            self.query_file = input("Введите имя файла с данными для задачи -> ")

            if self.query_file in list_file:
                return self.query_file

            else:
                print("Введен не верный файл")

    def get_query(self):
        self.select_file()
        list_query = []
        with open(self.query_file) as data_file:
            for line in data_file:
                list_query.append(line.replace('\n', ''))

        return list_query

    def get_serp(self, query, date):
        current_date = dt.strptime(date, '%m-%Y') - timedelta(days=30)
        now_date = dt.today()
        str_date = now_date.strftime('%m-%Y')
        now_date = dt.strptime(str_date, '%m-%Y')

        domain_list = []

        request_string = "http://api.megaindex.com/visrep/month?"

        while current_date <= now_date:
            request_string += "dates[]="+current_date.strftime('%m-%Y')+"&"

            current_date += timedelta(days=30)

        correct_request_string = request_string[0:len(request_string)-1]
        self.data['word'] = query
        self.data['ser_id'] = '1'
        i = 0
        while True:
            try:
                response = requests.get(correct_request_string, params=self.data, timeout=60)
                with open('serp_log.json', 'w') as log_file:
                    json.dump(response.json(), log_file)

                if 'error' not in response.json():
                    for element in response.json()['data']:
                        for position in response.json()['data'][element]:
                            domain_list.append(position['domain'])
                    return set(domain_list)

                else:
                    return False

            except:
                print("Ошибка получения серпа. Повтор попытки")
                sleep(2**i)
                i += 1

    def check_whois(self, domains):
        free_domains = []
        for domain in domains:
            i = 1
            correct_domain = re.sub(r'([a-z0-9\.]+)\.(\w+)\.(\w+)', r'\2.\3', domain)
            while True:
                try:
                    response = requests.get('https://www.nic.ru/whois/?searchWord='+correct_domain, timeout=60)
                    print(domain, response)
                    break
                except:
                    sleep(2**i)
                    pass

            if re.search(correct_domain + ' занят', response.text):
                with open('fail.log', 'a') as fail_data:
                    string_to_write = domain + ' - ' + dt.now().strftime("%Y-%m-%d-%H:%M:%S")+'\n'
                    fail_data.write(string_to_write)

                sleep(10)

            else:
                free_domains.append(correct_domain)
                sleep(5)

        return free_domains

    def check_backlinks(self, domain):
        self.data['domain'] = domain
        self.data['sort'] = 'domain_rank'
        self.data['desc'] = '1'
        i = 0
        while True:
            try:
                response = requests.get('http://api.megaindex.com/backlinks', params=self.data, timeout=60)
                with open('backlinks_log.json', 'w') as log_file:
                    json.dump(response.json(), log_file)

                break

            except:
                print("Ошибка получения обратных ссылок. Повтор попытки")
                sleep(2 ** i)
                i += 1

        if 'total' in response.json().keys():
            total_list = []
            anchor_list = []
            for item in response.json()['data']:
                anchor_list.append(item['link_text'])

            total_list.append(response.json()['total']['links_unique'])
            total_list.append(anchor_list)

            return total_list

        else:
            return ['', []]

    def get_domain_links(self, domains):
        count_domain_links_list = {}
        for domain in domains:
            count_domain_links_list[domain] = self.check_backlinks(domain)
            sleep(3)

        return count_domain_links_list

    def write_to_csv(self, total_dict):
        backlinks_info = 'result_data_' + dt.now().strftime("%Y-%m-%d-%H:%M:%S") + '.csv'
        with open(backlinks_info, 'w', encoding="utf-8") as result_file:
            result_file.write('domain\tcount backlinks\tanchors\n')
        for domain in total_dict:
            anchor_string = '; '.join(total_dict[domain][1])
            result_string = domain + '\t' + str(total_dict[domain][0]) + '\t' + anchor_string + '\n'

            with open(backlinks_info, 'a', encoding="utf-8") as result_file:
                result_file.write(result_string)

        print("Результаты в файле ->", backlinks_info)

    def start(self):
        print("Приложение поиск дропов по истории выдачи")
        date = input("Введите месяц и год, с которой сканировать выдачу (MM-YYYY) - > ")
        queries = self.get_query()
        finaly_list = []
        for query in queries:
            print("Сканируется запрос -> ", query)
            if self.get_serp(query, date):
                finaly_list.extend(self.check_whois(self.get_serp(query, date)))
            else:
                print("Такого запроса нет в базе")
                pass

        print(self.get_domain_links(list(set(finaly_list))))

        self.write_to_csv(self.get_domain_links(list(set(finaly_list))))


if __name__ == "__main__":
    data = {}
    data['key'] = '8753f30aae23f23f44403428b512b3a5'
    ya_s = Drop(data)

