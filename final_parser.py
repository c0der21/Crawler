import urllib.request
import re
import threading
import queue
from urllib.parse import urljoin, urlparse
import random as rnd


class SiteMapGenerator:
    '''
    Основной класс генерации карты сайта
    '''
    def __init__(self, root_url, max_depth=3, num_threads=10, max_requests=100, output_file = None):
        self.root_url = root_url
        self.max_depth = max_depth
        self.num_threads = num_threads
        self.urls_seen = []
        self.q = queue.Queue()
        self.q.put((root_url, 0))
        self.max_requests = max_requests
        self.request_count = 0
        self.output_file = output_file
    
    def generate_sitemap(self):
        '''
        Функция генерации карты сайта, используя разные потоки
        '''
        threads = []
        for _ in range(self.num_threads):
            t = threading.Thread(target=self.worker)
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()

        if self.output_file:
            self.write_to_file()

    def write_to_file(self):
        '''
        Функция записи просмотренных адресов в файл
        '''
        with open(self.output_file, 'w') as f:
            for url in self.urls_seen:
                f.write(url + '\n')

    def is_valid_url(self, url):
        '''
        Проверка правильности адреса
        '''
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False
        
    def has_same_base_url(self, url):
        '''
        Проверка на повторение адреса
        '''
        return urlparse(url).netloc == urlparse(self.root_url).netloc

    def worker(self):
        '''
        Основаня функция скролинга сайта
        '''
        while not self.q.empty() and self.request_count < self.max_requests:
            link, level = self.q.get()
            print(link, level)

            if level > self.max_depth:
                continue

            if link in self.urls_seen:
                continue

            if not self.is_valid_url(link):
                continue

            if not self.has_same_base_url(link):
                continue

            self.urls_seen.append(link)
            self.request_count += 1

            time.sleep(4 + 3 * rnd.random())
            page_content = self.get_html(link)
            linked_urls = self.get_links(page_content)

            for url in linked_urls:
                if self.is_valid_url(url) and self.has_same_base_url(url) and url not in self.urls_seen \
                        and self.request_count < self.max_requests:
                    self.q.put((url, level+1))

    def get_html(self, url):
        '''
        Получение из запроса html, для последующего извлечения адресов
        '''
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                return response.read().decode('utf-8', errors='ignore')
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            return ""
    
    def get_links(self, html):
        '''
        Получение адресов из html, с использованием регулярного выражения
        '''
        pattern = r'<a[^>]*href=[\'"]([^\'"]+)[\'"][^>]*>'
        for match in re.findall(pattern, html):
            yield self.normalize_link(match)
    
    def normalize_link(self, link):
        '''
        Нормализация адреса, исходя из того является он относительным или абсолютным
        '''
        if link.startswith('/'):
            return urljoin(self.root_url, link)
        elif link.startswith('http'):
            return link
        else:
            return ""


if __name__ == '__main__':
    import time

    root_urls = [
        "http://crawler-test.com/", # 3285.7475869283891 секунд
        "http://google.com/", # 907.213484209533 секунд
        "https://vk.com", #  3155.8327152729034 секунд
        "https://dzen.ru", # 2275.303955783921 секунд
        "https://stackoverflow.com" # 3214.436811685562 секунд
    ]
    
    for root_url in root_urls:
        try:
            domain = urlparse(root_url).netloc
            output_file = f"{domain}_sitemap.txt"
            sitemap_generator = SiteMapGenerator(root_url, output_file=output_file, max_requests=500, max_depth=6)  
            start_time = time.time()
            sitemap_generator.generate_sitemap()
            end_time = time.time()
            elapsed_time = end_time - start_time
            print("Время выполнения скрипта: ", elapsed_time, "секунд")

        except Exception as Ex:
            print(f"Сайт {root_url} не получилось отсканировать")
            print(Ex)




# Здесь SiteMapGenerator - класс, который представляет генератор карты сайта. При создании экземпляра класса передается корневой URL сайта,
# максимальная глубина, до которой следует обходить структуру сайта, и количество потоков для обработки.
# Обход структуры реализован в методе worker. При обходе каждый URL добавляется в множество self.urls_seen, чтобы избежать повторных обращений к нему.
# Метод get_html получает HTML-код страницы по URL с использованием модуля urllib. 
# Метод get_links извлекает ссылки из HTML-кода с помощью регулярного выражения и возвращает их в виде генератора. Метод normalize_link преобразует относительные ссылки в аб
