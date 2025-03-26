import requests
from bs4 import BeautifulSoup
import json
import logging
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configure logging to write logs to app.log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),  # Logs will be written to app.log
        logging.StreamHandler()         # Logs will also be printed to console
    ]
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.regalrexnord.com/products"
FILENAME = "product_names.json"

def scrape_product_names(url):
    """Scrapes product names and their subproducts from the given URL."""
    try:
        logger.info(f"Accessing base URL: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        products = []

        # Find all main product tiles
        for i, product in enumerate(soup.find_all('article', class_='product-tiles__tile')):
            # if i in [0, 2]:
                try:
                    # Extract title from h2 or h3
                    title_tag = product.find('h3') or product.find('h2')
                    title = title_tag.get_text(strip=True) if title_tag else "Unknown Title"

                    # Extract product link
                    product_link = product.find('a', href=True)['href']
                    full_product_link = f"https://www.regalrexnord.com{product_link}"

                    logger.info(f"Found main product: {title} | Link: {full_product_link}")

                    # Scrape subproducts for this product
                    subproducts = scrape_subproducts(full_product_link)

                    products.append({
                        'product_name': title,
                        'product_link': full_product_link,
                        'subproducts': subproducts
                    })
                except AttributeError as e:
                    logger.warning(f"Skipping product due to missing data: {e}")
                    continue

        return products
    except Exception as e:
        logger.error(f"Error scraping products from {url}: {str(e)}")
        return []

def scrape_subproducts(url):
    """Scrapes subproduct names and their sub-subproducts from the given product page URL."""
    try:
        logger.info(f"Accessing subproduct URL: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        subproducts = []

        # Find all subproduct tiles
        for subproduct in soup.find_all('article', class_='product-tiles__tile'):
            try:
                # Extract subproduct title from h3
                subproduct_title_tag = subproduct.find('h3') or subproduct.find('h2')
                subproduct_title = subproduct_title_tag.get_text(strip=True) if subproduct_title_tag else "Unknown Sub-Title"
                
                # Extract subproduct link
                subproduct_link = subproduct.find('a', href=True)['href']
                full_subproduct_link = f"https://www.regalrexnord.com{subproduct_link}"
                
                logger.info(f"Found subproduct: {subproduct_title} | Link: {full_subproduct_link}")
                
                # Scrape sub-subproducts for this subproduct
                sub_subproducts = scrape_sub_subproducts(full_subproduct_link)
                
                subproducts.append({
                    'subproduct_name': subproduct_title,
                    'subproduct_link': full_subproduct_link,
                    'sub_subproducts': sub_subproducts
                })
            except AttributeError:
                logger.warning(f"Skipping subproduct due to missing data in {url}")
                continue

        return subproducts
    except Exception as e:
        logger.error(f"Error scraping subproducts from {url}: {str(e)}")
        return []

def scrape_sub_subproducts(url):
    """Scrapes sub-subproduct names and links from the given subproduct page URL."""
    try:
        logger.info(f"Accessing sub-subproduct URL: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        sub_subproducts = []

        # Find all result items
        for result_item in soup.find_all('div', class_='result-item'):
            try:
                # Extract sub-subproduct name
                sub_subproduct_name_tag = result_item.find('atomic-result-text', {'field': 'displayname'})
                sub_subproduct_name = sub_subproduct_name_tag.get_text(strip=True) if sub_subproduct_name_tag else "Unknown Sub-Subproduct"
                
                # Extract sub-subproduct link
                sub_subproduct_link_tag = result_item.find('a', href=True)
                sub_subproduct_link = sub_subproduct_link_tag['href'] if sub_subproduct_link_tag else "#"
                full_sub_subproduct_link = f"https://www.regalrexnord.com{sub_subproduct_link}"
                
                logger.info(f"Found sub-subproduct: {sub_subproduct_name} | Link: {full_sub_subproduct_link}")
                
                sub_subproducts.append({
                    'sub_subproduct_name': sub_subproduct_name,
                    'sub_subproduct_link': full_sub_subproduct_link
                })
            except AttributeError:
                logger.warning(f"Skipping sub-subproduct due to missing data in {url}")
                continue

        return sub_subproducts
    except Exception as e:
        logger.error(f"Error scraping sub-subproducts from {url}: {str(e)}")
        return []

def save_to_json(product_data, filename=FILENAME):
    """Saves product data to a JSON file."""
    try:
        with open(filename, "w") as f:
            json.dump({"products": product_data}, f, indent=4)
        logger.info(f"Saved {len(product_data)} products to {filename}")
    except Exception as e:
        logger.error(f"Failed to save product data to JSON: {e}")

def main():
    product_data = scrape_product_names(BASE_URL)
    save_to_json(product_data, FILENAME)

if __name__ == "__main__":
    main()