import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров с маркетплейса Ozon.

    Args:
        last_id (str): последний загруженный идентификатор
        client_id (str): идентификатор клиента
        seller_token (str): токен продавца

    Returns:
        str: значение по ключу "result" из словаря response_object

    Examples:
        >>> last_id = ""
        >>> client_id = "1234"
        >>> seller_token = "abcd1312"
        >>> get_product_list(last_id, client_id, seller_token)
        "{
            'id':1,
            'name': 'Тестовый товар',
            'offer_id':'offer_id1',
            'stock': 4,
            'price':999
        }"
        >>> last_id = ""
        >>> client_id = "1234"
        >>> seller_token = "wrong_token"
        >>> get_product_list(last_id, client_id, seller_token)
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url:
            https://api-seller.ozon.ru/v2/product/list
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров с маркетплейса Ozon.

    Args:
        client_id (str): идентификатор клиента
        seller_token (str): токен продавца

    Returns:
        list: список артикулов товаров

    Examples:
        >>> client_id = "1234"
        >>> seller_token = "abcd1312"
        >>> get_offer_ids(client_id, seller_token)
        ['offer_id1','offer_id2']
        >>> client_id = "1234"
        >>> seller_token = "wrong_token"
        >>> get_offer_ids(client_id, seller_token)
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url:
            https://api-seller.ozon.ru/v2/product/list
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров на маркетплейсе Ozon.

    Args:
        prices (list): список словарей с ценами на товары
        client_id (str): идентификатор клиента
        seller_token (str): токен продавца

    Returns:
        dict: возвращает словарь ответа на запрос в JSON-формате

    Examples:
        >>> prices = [
                {'Код':'1234', 'Цена':'999'},
                {'Код':'12345', 'Цена':'9998'}
            ]
        >>> client_id = "1234"
        >>> seller_token = "abcd1312"
        >>> update_price(prices, client_id, seller_token)
        {
            "product_id": 1234,
            "offer_id": "offer_id1",
            "updated": true,
            "errors": [ ]
        }
        >>> prices = [
                {'Код':'1234', 'Цена':'999'},
                {'Код':'12345', 'Цена':'9998'}
            ]
        >>> client_id = ""
        >>> seller_token = "abcd1312"
        >>> update_price(prices, client_id, seller_token)
        {"code":16,"message":"Client-Id and Api-Key headers are required"}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров на маркетплейсе Ozon.

    Args:
        stocks (list): список словарей с количеством товаров
        client_id (str): идентификатор клиента
        seller_token (str): токен продавца

    Returns:
        dict: возвращает словарь ответа на запрос в JSON-формате

    Examples:
        >>> stocks = [
            {'Код':'1234', 'Количество':'4'},
            {'Код':'12345', 'Количество':'3'}
        ]
        >>> client_id = "1234"
        >>> seller_token = "abcd1312"
        >>> update_stocks(stocks, client_id, seller_token)
        {
        "product_id": 1234,
        "offer_id": "offer_id1",
        "updated": true,
        "errors": [ ]
        }
        >>> stocks = [
            {'Код':'1234', 'Количество':'4'},
            {'Код':'12345', 'Количество':'3'}
        ]
        >>> client_id = ""
        >>> seller_token = "abcd1312"
        >>> update_stocks(stocks, client_id, seller_token)
        {"code":16,"message":"Client-Id and Api-Key headers are required"}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать архив ostatki.zip с сайта и прочитать содержимое Excel файла.

    Returns:
        list: список словарей с данными по каждому товару

    Examples:
        >>> download_stock()
        [{'Код':'1234','Наименование':'часы','Цена':'9999','Количество':'3'}]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Заполнить остатки товаров для загрузки на маркетплейс Ozon.

    Args:
        watch_remnants (list): данные по наличию моделей
        offer_ids (list): артикулы товаров, их количество и цены

    Returns:
        list: артикулы товаров и их количество

    Examples:
        >>> watch_remnants = [
                {'Код':'1234', 'Количество':'4'},
                {'Код':'12345', 'Количество':'3'}
            ]
        >>> offer_ids = ['1234','2346']
        >>> create_stocks(watch_remnants, offer_ids)
        [
            {'offer_id':'1234', 'stock':4},
            {'offer_id':'12345', 'stock':3},
            {'offer_id':'2346', 'stock':0}
        ]
        >>> watch_remnants = []
        >>> offer_ids = []
        >>> create_stocks(watch_remnants, offer_ids)
        []
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Заполнить цены товаров для загрузки на маркетплейс Ozon.

    Args:
        watch_remnants (list): данные по товарам
        offer_ids (list): артикулы товаров и

    Returns:
        list: артикулы товаров и их цены

    Examples:
        >>> watch_remnants = [
                {'Код':'1234', 'Цена':'4000.0'},
                {'Код':'12345', 'Цена':'3000.0'}
            ]
        >>> offer_ids = ['1234','2346']
        >>> create_prices(watch_remnants, offer_ids)
        [
             {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "1234",
                "old_price": "0",
                "price": 4000),
            }
        ]
        >>> watch_remnants = []
        >>> offer_ids = []
        >>> create_prices(watch_remnants, offer_ids)
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену, отбросив всё после первой точки и удалив всё, кроме цифр.

    Args:
        price (str): значение цены

    Returns:
        str: цена, состоящая только из цифр, без дробной части

    Examples:
        >>> price = "5'990.00"
        >>> price_conversion(price)
        5990
        >>> price = "5.990.00"
        >>> price_conversion(price)
        5
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Args:
        lst (list): исходный список
        n (int): количество элементов в выходном списке

    Yields:
        list: список из n элементов

    Examples:
        >>> mylist = [1,2,3,4,5,6]
        >>> divider = 3
        >>> for x in list(divide(mylist, divider)):
        >>>     print(x)
        [1,2,3]
        [4,5,6]
        >>> mylist = [1,2,3,4,5,6]
        >>> divider = 0
        >>> for x in list(divide(mylist, divider)):
        >>>     print(x)
        for i in range(0, len(lst), n):
        ValueError: range() arg 3 must not be zero
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно загружает цены на товары.

    Args:
        watch_remnants: информация о товарах
        client_id: идентификатор клиента
        seller_token: токен продавца

    Returns:
        list: список цен

    Examples:
        >>> watch_remnants = [
                {'Код':'1234', 'Цена':'4000.0'},
                {'Код':'12345', 'Цена':'3000.0'}
            ]
        >>> client_id = '123123'
        >>> seller_token = 'abc123'
        >>> upload_prices(watch_remnants, client_id, seller_token)
        [
             {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "1234",
                "old_price": "0",
                "price": 4000),
            },
             {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "12345",
                "old_price": "0",
                "price": 3000),
            },
        ]
        >>> watch_remnants = [
                {'Код':'1234', 'Цена':'4000.0'},
                {'Код':'12345', 'Цена':'3000.0'}
            ]
        >>> client_id = '123123'
        >>> seller_token = 'wrong_token'
        >>> upload_prices(watch_remnants, client_id, seller_token)
        []
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно загрузить информацию об остатках товаров.

    Args:
        watch_remnants: информация о товарах
        client_id: идентификатор клиента
        seller_token: токен продавца

    Returns:
        tuple: кортеж из непустых остатков и всех остатков

    Examples:
        >>> watch_remnants = [
                {'Код':'1234', 'Количество':'4'},
                {'Код':'12345', 'Количество':'3'}
            ]
        >>> client_id = '123123'
        >>> seller_token = 'abc123'
        >>> upload_stocks(watch_remnants, client_id, seller_token)
        (
            [
                {'offer_id':'1234', 'stock':4},
                {'offer_id':'12345', 'stock':3}
            ],
            [
                {'offer_id':'2346', 'stock':0}
            ]
        )
        >>> watch_remnants = []
        >>> client_id = '123123'
        >>> seller_token = 'wrong_token'
        >>> upload_stocks(watch_remnants, client_id, seller_token)
        (
            [],
            []
        )
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Обновить цены и остатки товаров на маркетплейсе Ozon.

    Загружает с сайта-производителя и обновляет информацию об остатках и ценах
    товаров на маркетплейсе Ozon.
    Если возникают ошибки, выводит соответствующие сообщения.

    Raises:
        requests.exceptions.ReadTimeout: превышено время ожидания запроса.
        requests.exceptions.ConnectionError: возникла ошибка соединения.
        Exception: любая другая необработанная ошибка.
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
