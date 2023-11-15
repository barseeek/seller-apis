# seller-apis
Данный репозиторий хранит в себе скрипты для автоматизации процесса обновления цен и остатков для маркетплейсов Yandex Market и Ozon.
## market.py
Скрипт получает артикулы товаров из магазина на маркетплейсе Yandex Market для двух моделей: FBS и DBS, выгружает таблицу товаров с сайта магазина часов и обновляет информацию по остаткам и ценам на Yandex Market для двух моделей.
#### Конфигурация market.py

Убедитесь, что установлены следующие переменные окружения:

- `MARKET_TOKEN`: токен доступа к Яндекс.Маркету.
- `FBS_ID`: ID кампании для FBS.
- `DBS_ID`: ID кампании для DBS.
- `WAREHOUSE_FBS_ID`: ID склада для FBS.
- `WAREHOUSE_DBS_ID`: ID склада для DBS.

## seller.py

Скрипт получает артикулы товаров из магазина на маркетплейсе Ozon, выгружает таблицу товаров с сайта магазина часов и обновляет информацию по остаткам и ценам на Ozon.

#### Конфигурация seller.py

Убедитесь, что установлены следующие переменные окружения:

- `SELLER_TOKEN"`: токен продавца с Озон.
- `CLIENT_ID`: ID клиента Озон.