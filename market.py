import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список продуктов для указанной страницы кампании.

    Args:
        page (str): токен страницы для запроса
        campaign_id (str): идентификатор кампании
        access_token (str): токен доступа

    Returns:
        list: список продуктов для указанной страницы кампании

    Examples:
    >>> get_product_list("123", "456", "a1b2c3")
        ['product_1', 'product_2']
    >>> get_product_list(123, "456", "a1b2c3")
    requests.exceptions.HTTPError: 400 Bad Request: nextpagetoken
      should be string type
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров.

    Args:
        stocks (list): информация об остатках товаров
        campaign_id (str): идентификатор кампании
        access_token (str): токен доступа

    Returns:
        dict: ответ от сервера в формате JSON с информацией об остатках

    Examples:
        >>> update_stocks(
            [
            {
                "sku": "string",
                "warehouseId": 0,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": "2022-12-29T18:02:01Z"
                    }
                ]
            }
            ],
            "campaign_123", "access_token")

        {"status": "OK"}

        >>> update_stocks([], "campaign_123", "access_token")
        {
            "status": "OK",
            "errors": [
                {
                    "code": "string",
                    "message": "skus can't be null"
                }
            ]
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены на товары.

    Args:
        prices (list): информация о ценах на товары
        campaign_id (str): идентификатор кампании
        access_token (str): токен доступа

    Returns:
        dict: ответ от сервера в формате JSON с информацией о ценах

    Examples:
        >>> update_price(
            [
            {
                "offerId": "string",
                "id": "string",
                "feed": {
                    "id": 0
                },
                "price": {
                    "value": 0,
                    "discountBase": 0,
                    "currencyId": "RUR",
                    "vat": 0
                },
                "marketSku": 0,
                "shopSku": "string"
            }
            ],
            "campaign_123", "access_token")

        {"status": "OK"}

        >>> update_prices([], "campaign_123", "access_token")
        {
            "status": "OK",
            "errors": [
                {
                    "code": "string",
                    "message": "offers can't be null"
                }
            ]
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс Маркета для заданной кампании.

    Args:
        campaign_id (str): идентификатор кампании
        market_token (str): токен доступа к Яндекс Маркету

    Returns:
        list: список артикулов товаров для заданной кампании

    Examples:
        >>> get_offer_ids("campaign_123", "market_token")
        ['offerid_1', 'offerid_2']
        >>> get_offer_ids("campaign_123", "wrong_market_token")
        []
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать информацию об остатках товаров.

    Args:
        watch_remnants (list): информация о товарах
        offer_ids (list): список артикулов товарови
        warehouse_id (str): идентификатор складаи

    Returns:
        list: информация об остатках товаров.

    Examples:
        >>> create_stocks(
            [
                {'Код':'offer_1', 'Количество':'4'},
                {'Код':'offer_2', 'Количество':'3'}
            ],
            ["offer_1", "offer_2"], "warehouse_123")

        [{'id': 'offer_1', 'stock': 50}, {'id': 'offer_2', 'stock': 75}]
        >>> create_stocks([], [], "warehouse_123")
        []
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать информацию о ценах товаров.

    Args:
        watch_remnants (list): информация о товарах
        offer_ids (list): список артикулов товаров

    Returns:
        list: информация о ценах товаров

    Examples:
        >>> create_prices(
            [
                {'Код':'offer_1', 'Цена':'100'},
                {'Код':'offer_2', 'Цена':'159'}
            ],
            ["offer_1", "offer_2"])

        [
            {'id': 'offer_1', 'price': {"value": 100, "currencyId": "RUR"},
            {'id': 'offer_2', 'price': {"value": 159, "currencyId": "RUR"}
        ]
        >>> create_prices([],[])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить цены на товары.

    Args:
        watch_remnants (list): информация о товарах
        campaign_id (str): идентификатор кампании
        market_token (str): токен доступа к Яндекс Маркету

    Returns:
        list: список цен на товары

    Examples:
        >>> await upload_prices(
            [
                {'Код':'offer_1', 'Цена':'100'},
                {'Код':'offer_2', 'Цена':'159'}
            ]
            , "campaign_123", "market_token")

        [
            {'id': 'offer_1', 'price': {"value": 100, "currencyId": "RUR"},
            {'id': 'offer_2', 'price': {"value": 159, "currencyId": "RUR"}
        ]

        >>> await upload_prices([
            [
                {'Код':'offer_1', 'Цена':'100'},
                {'Код':'offer_2', 'Цена':'159'}
            ]
            ], "campaign_123", "wrong_market_token")
        []
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить информацию об остатках товаров.

    Args:
        watch_remnants (list): информация о товарах
        campaign_id (str): идентификатор кампании
        market_token (str): токен доступа к Яндекс Маркету
        warehouse_id (str): идентификатор склада

    Returns:
        tuple: кортеж из списков непустых остатков и всех остатков.

    Example:
        >>> await upload_stocks(
            [
                {'Код':'offer_1', 'Цена':'100'},
                {'Код':'offer_2', 'Цена':'159'}
            ]
            , "campaign_123", "market_token", "warehouse_123")

        (
            [
                {'id': 'offer_1', 'stock': 50},
                {'id': 'offer_2', 'stock': 75}
            ],
            [
                {'id': 'offer_1', 'stock': 50},
                {'id': 'offer_2', 'stock': 75},
                {'id': 'offer_3', 'stock': 0}
            ]
        )
        >>> await upload_stocks(
            [
                {'Код':'offer_1', 'Цена':'100'},
                {'Код':'offer_2', 'Цена':'159'}
            ]
            , "campaign_123", "wrong_market_token", "warehouse_123")
        ([],[])
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Обновить цены и остатки товаров на Яндекс Маркете.

    Использует данные о товарах, кампаниях, и токенах для обновления
    информации о ценах и остатках товаров на платформе Яндекс Маркет.
    Для каждой кампании получает и обрабатывает список остатков и цен,
    обновляя их на платформе. При возникновении ошибок выводит сообщение
    об ошибке и продолжает работу с остальными кампаниями.

    Raises:
        requests.exceptions.ReadTimeout: превышено время ожидания запроса.
        requests.exceptions.ConnectionError: возникла ошибка соединения.
        Exception: любая другая необработанная ошибка.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
