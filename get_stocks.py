import requests
import bs4

urls = {
    'sp500': 'http://slickcharts.com/sp500',
    'dj': 'http://slickcharts.com/dowjones0',
    'nq100': 'http://slickcharts.com/nasdaq100'
}

stocks_cache = {}


def get_sp500():
    global stocks_cache

    if not stocks_cache:
        html = requests.get(urls['sp500'])
        # print(html.content)
        bs = bs4.BeautifulSoup(html.content, "lxml")

        symbols = bs.find_all('input', {'name': 'symbol'})
        # print(symbols)
        stocks = {}
        for symbol in ['AMD', 'XIV', 'NVDA']:
            # print(symbol.attrs['value'])
            stocks[symbol] = {
                'auto': {
                    'chart': 1,
                    'chart_inv': 30,
                    'lv1': 1
                }
            }
        stocks_cache = stocks
    return stocks_cache
