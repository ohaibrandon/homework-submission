import requests
import json
import base64
import time
from datetime import datetime, timezone
from functools import lru_cache
from urllib import parse

class ShopifyApp:
    API_VERSION = '2020-07'

    def __init__(self, company, url, token, password):
        self.company = company
        self.url = url
        self.token = token
        self.password = password

    #
    # FETCH SHOPIFY DATA
    # --------------------------------------------------

    def get_store_url(self, api_version=API_VERSION):
        """ Checks to make sure URL is valid. 
            Returns `store_url` which can be used for requests made to Shopify.
        """

        url = parse.urlparse(self.url)

        if not url.netloc.endswith('myshopify.com'):
            raise ValueError('URL must be formatted like mystore.myshopify.com.') 

        store_url = 'https://{}:{}@{}/admin/api/'.format(self.token, self.password, url.netloc) + api_version

        return store_url

    def get_orders_historical(self, store_url):
        """ Returns the raw data of all Shopify orders.
        Args: 
            store_url (str): fetched from the `get_store_url` method.
            task (func): name of the sync that determines if `created_at_min` param should be used.
        """
        
        orders = requests.get(store_url + '/orders.json').json()['orders']

        return orders

    def get_orders_periodic(self, store_url):
        """ Returns the raw data of all Shopify orders in the last 30 minutes.
        """

        # subtract 30 minutes from the current time in the ISO 8601 format 
        past_time = datetime.fromtimestamp(datetime.timestamp(datetime.now()) - 1800)
        past_dt = datetime(past_time.year, past_time.month, past_time.day, past_time.hour, past_time.minute, past_time.second, tzinfo=timezone.utc).isoformat()
        orders = requests.get(store_url + '/orders.json?created_at_min=' + past_dt).json()['orders']

        return orders

    def get_collections(self, store_url):
        """ Returns the raw data of every `collection_id`.
        """

        collections = requests.get(store_url + '/collects.json').json()['collects']

        return collections

    @lru_cache(maxsize=512)
    def get_collection_name(self, store_url, collect_id):
        """ Takes in a `collect_id` and returns a collection name that corresponds to it.
        Args:
            collect_id (str): same as `collection_id` fetched from the /collects.json endpoint. 
        """

        # this is a very repetitive task that ends up taking a lot of time 
        # caching helped tremendously with speeding up the entire operation

        collection_name = requests.get(store_url + '/collections/' + str(collect_id) + '.json').json().get('collection', {}).get('title')

        return collection_name

    @lru_cache(maxsize=512)
    def get_product(self, store_url, product_id):
        """ Takes in a `product_id` and returns a product object that corresponds to it.
        Args:
            product_id (str): added from `item['product_id']`. 
        """

        # Product image urls and handles are not availble from the order level.
        # They're available from the product level but a request has to be made for each item.
        
        # NOTE:
        # (1) This is an expensive operation.
        # (2) It sometimes returns NoneType values even when the product should absolutely have a value.
        # (3) Caching seems to resolve both of these issues.

        item_info = requests.get(store_url + '/products/' + str(product_id) + '.json').json().get('product', {})

        return item_info

    #
    # BUILD NEW OBJECTS
    # --------------------------------------------------

    def create_customer_properties(self, order):
        """ Takes in an order object and returns `customer_properties` for the `Ordered Product` event.
        Args:
            order (dict): raw order data fetched from Shopify's Orders API.
        """

        customer = order['customer']

        customer_properties = {
            '$email': customer['email'],
            '$first_name': customer.get('first_name'),
            '$last_name': customer.get('last_name')
        }

        return customer_properties

    def update_customer_properties(self, order, customer_properties):
        """ Takes in `customer_properties` and updates it with `add_customer_properties` 
            and then returns the updated `customer_properties` for the `Placed Order` event.
        Args:
            customer_properties (dict): fetched from the `create_customer_properties` method.
        """

        customer = order['customer']
        
        add_customer_properties = {
            '$phone_number': customer.get('phone'),
            '$address1': customer.get('default_address', {}).get('address1'),
            '$address2': customer.get('default_address', {}).get('address2'),
            '$city': customer.get('default_address', {}).get('city'),
            '$zip': customer.get('default_address', {}).get('zip'),
            '$region': customer.get('default_address', {}).get('province'),
            '$country': customer.get('default_address', {}).get('country')
        }

        customer_properties.update(add_customer_properties)
        
        return customer_properties

    def create_billing_address(self, order):
        """ Takes in an order object and returns the billing address of the order.
        """

        order_billing = order.get('billing_address', {})

        billing_info = {
            'FirstName': order_billing.get('first_name', ""), 
            'LastName': order_billing.get('last_name', ""), 
            'Company': order_billing.get('company', ""), 
            'Address1': order_billing.get('address1', ""), 
            'Address2': order_billing.get('address2', ""), 
            'City': order_billing.get('city', ""), 
            'Region': order_billing.get('province', ""), 
            'RegionCode': order_billing.get('province_code', ""), 
            'Country': order_billing.get('country', ""), 
            'CountryCode': order_billing.get('country_code', ""), 
            'Zip': order_billing.get('zip', ""), 
            'Phone': order_billing.get('phone', "")
        }

        return billing_info

    def create_shipping_address(self, order):
        """ Takes in an order object and returns the shipping address of the order.
        """

        order_shipping = order.get('shipping_address', {})

        shipping_info = {
            'FirstName': order_shipping.get('first_name', ""), 
            'LastName': order_shipping.get('last_name', ""), 
            'Company': order_shipping.get('company', ""), 
            'Address1': order_shipping.get('address1', ""), 
            'Address2': order_shipping.get('address2', ""), 
            'City': order_shipping.get('city', ""), 
            'Region': order_shipping.get('province', ""), 
            'RegionCode': order_shipping.get('province_code', ""), 
            'Country': order_shipping.get('country', ""), 
            'CountryCode': order_shipping.get('country_code', ""), 
            'Zip': order_shipping.get('zip', ""), 
            'Phone': order_shipping.get('phone', "")
        }

        return shipping_info

    def create_timestamp(self, order):
        """ Takes in an order object, looks up the `created_at` value, and turns it into a unix timestamp.
        """

        timestamp = int(datetime.strptime(order['created_at'], '%Y-%m-%dT%H:%M:%S%z').timestamp())

        return timestamp

    def create_discount_codes_list(self, order):
        """ Takes in an order object and returns a list of discount codes.
        """

        discounts = []

        # append any applied discount codes to the `discounts` list
        for code in order['discount_codes']:
            discounts.append(code['code'])

        return discounts

    def create_collect_ids_list(self, order, collections):
        """ Takes in an order object and the raw collections data to find a collection with the same 
            `product_id`. If true, append its `collection_id` to the `collect_ids` list.
        """

        collect_ids = []

        for item in order['line_items']:
            for collection in collections:
                if item['product_id'] == collection['product_id']:
                    collect_ids.append(collection['collection_id'])

        return collect_ids

    def create_categories_list(self, store_url, order, collect_ids):
        """ Takes in an order object and `collect_ids` list fetched from the `create_collect_ids_list` 
            method. This method will be used to append the name of each collection to the `categories` 
            list.
        """

        categories = []

        for item in order['line_items']:

            for collect_id in collect_ids:
                collection_name = self.get_collection_name(store_url, collect_id)
                if collection_name and not collection_name in categories:  # ignore dupes
                    categories.append(collection_name)

        return categories

    def create_item_names_list(self, order):
        """ Takes in an order object and returns a list of item names.
        """

        item_names = []

        for item in order['line_items']:
            item_names.append(item['name'])

        return item_names

    def create_vendor_list(self, order):
        """ Takes in an order object and returns a list of vendor names.
        """

        brands = []

        for item in order['line_items']:
            if not item['vendor'] in brands:  # ignore dupes
                brands.append(item['vendor'])

        return brands

    def create_image_url(self, item_info):
        """ Takes in `item_info` and returns the first image of a product.  
        Args:
            item_info (dict): fetched from the `get_product` method that contains
            additional info about a specific product ID, such as image URLs and handles. 
        """

        first_image = ""

        try:
            first_image = item_info.get('images', [{}])[0].get('src')
        except IndexError:  # ignore IndexError if a product has nothing in `images`
            pass

        return first_image

    def create_product_url(self, store_url, item_info):
        """ Takes in `store_url` and `item_info` to return a product page URL.  
        """

        product_url = store_url + '/products/' + str(item_info.get('handle'))
        
        return product_url

    def create_items_array(self, order, product_url, first_image, categories):
        items = []

        for item in order['line_items']:
            product = {
                'ProductID': item['product_id'], 
                'SKU': item['sku'], 
                'ProductName': item['name'], 
                'Quantity': item['quantity'], 
                'ItemPrice': item['price'], 
                'RowTotal': item['price'], 
                'ProductURL': product_url, 
                'ImageURL': first_image, 
                'Categories': categories, 
                'Brand': item['vendor']
            }

            items.append(product)

        return items

    def create_product_properties(self, order, product_url, first_image, categories):
        for item in order['line_items']:
            product_properties = {
                '$event_id': item['id'],
                '$value': item['price'],
                'ProductID': item['product_id'],
                'SKU': item['sku'],
                'ProductName': item['name'],
                'Quantity': item['quantity'],
                'ProductURL': product_url,
                'ImageURL': first_image,
                'ProductCategories': categories,
                'ProductBrand': item['vendor']
            }

        return product_properties

    def create_order_properties(self, order, categories, item_names, brands, discounts, items, billing, shipping):
        order_properties = {
            '$event_id': order['id'],
            '$value': order['total_price'],
            'Categories': categories,
            'ItemNames': item_names,
            'Brands': brands,
            'DiscountCode': discounts,
            'DiscountValue': order['total_discounts'],
            'Items': items,
            'billing_address': billing,
            'shipping_address': shipping
        }

        return order_properties

    def create_product_payload(self, order, customer_properties, product_properties, timestamp):
        product_payload = {
            'token': self.company, 
            'event': 'Ordered Product', 
            'customer_properties': customer_properties, 
            'properties': product_properties, 
            'time': timestamp
        }

        return product_payload

    def create_order_payload(self, order, customer_properties, order_properties, timestamp):
        order_payload = {
            'token': self.company, 
            'event': 'Placed Order', 
            'customer_properties': self.update_customer_properties(order, customer_properties), 
            'properties': order_properties, 
            'time': timestamp
        }

        return order_payload

    # 
    # SET UP SYNCS
    # --------------------------------------------------

    def historical_orders_sync(self, api_version=API_VERSION):
        store_url = self.get_store_url()
        orders = self.get_orders_historical(store_url)
        collections = self.get_collections(store_url)
        counter = 0

        for order in orders:
            customer_properties = self.create_customer_properties(order)
            billing = self.create_billing_address(order)
            shipping = self.create_shipping_address(order)
            discounts = self.create_discount_codes_list(order)
            timestamp = self.create_timestamp(order)
            collect_ids = self.create_collect_ids_list(order, collections)
            categories = self.create_categories_list(store_url, order, collect_ids)

            for item in order['line_items']:
                item_info = self.get_product(store_url, item['product_id'])
                item_names = self.create_item_names_list(order)
                brands = self.create_vendor_list(order)
                first_image = self.create_image_url(item_info)
                product_url = self.create_product_url(store_url, item_info)
                items = self.create_items_array(order, product_url, first_image, categories)
                product_properties = self.create_product_properties(order, product_url, first_image, categories)
                product_payload = self.create_product_payload(order, customer_properties, product_properties, timestamp)
                self.track_event(product_payload)

            order_properties = self.create_order_properties(order, categories, item_names, brands, discounts, items, billing, shipping)
            order_payload = self.create_order_payload(order, customer_properties, order_properties, timestamp)
            self.track_event(order_payload)

            if not order['customer']['email'].endswith('@example.com'):  # skip counter if example email
                counter += 1
                print("Syncing orders... order count: {}".format(counter))

        print("Historical sync completed. Synced {} orders.".format(counter))

    def periodic_orders_sync(self, api_version=API_VERSION):
        sync_interval = 60 * 10  # syncs every 10 minutes
        start_time = time.time()

        while 1:    
            store_url = self.get_store_url()
            orders = self.get_orders_periodic(store_url)
            collections = self.get_collections(store_url)
            counter = 0

            for order in orders:
                customer_properties = self.create_customer_properties(order)
                billing = self.create_billing_address(order)
                shipping = self.create_shipping_address(order)
                discounts = self.create_discount_codes_list(order)
                timestamp = self.create_timestamp(order)
                collect_ids = self.create_collect_ids_list(order, collections)
                categories = self.create_categories_list(store_url, order, collect_ids)

                for item in order['line_items']:
                    item_info = self.get_product(store_url, item['product_id'])
                    item_names = self.create_item_names_list(order)
                    brands = self.create_vendor_list(order)
                    first_image = self.create_image_url(item_info)
                    product_url = self.create_product_url(store_url, item_info)
                    items = self.create_items_array(order, product_url, first_image, categories)
                    product_properties = self.create_product_properties(order, product_url, first_image, categories)
                    product_payload = self.create_product_payload(order, customer_properties, product_properties, timestamp)
                    self.track_event(product_payload)

                order_properties = self.create_order_properties(order, categories, item_names, brands, discounts, items, billing, shipping)
                order_payload = self.create_order_payload(order, customer_properties, order_properties, timestamp)
                self.track_event(order_payload)

                if not order['customer']['email'].endswith('@example.com'):  # skip counter if example email
                    counter += 1
                    print("Syncing orders... order count: {}".format(counter))

            print("Periodic sync completed. Running again in {} minutes".format(sync_interval))

            # calculate difference between the currrent time and `start_time`
            # modulo that by 60 and subtract `sync_interval` with it
            # if the sync takes longer than the interval then it will wait for its next turn
            time.sleep(sync_interval - ((time.time() - start_time) % 60))

    #
    # SEND KLAVIYO EVENTS
    # --------------------------------------------------

    def track_event(self, payload):
        """ Takes in a payload and records the event in Klaviyo.
            TODO: This should probably belong in a superclass object and turn ShopifyApp into a subclass. 
        Args:
            payload (dict): raw event payload
        """

        track_url = 'https://a.klaviyo.com/api/track?data='

        encoded_data = parse.quote(str(base64.b64encode(json.dumps(payload).encode('utf-8')))[slice(2, -1)])

        requests.get(track_url + encoded_data)