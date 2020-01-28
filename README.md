# wuhan_news_scrape

A python script run as crontab to get data from QQ news about the wuhan coronavirus
endpoint.

- 'global' : 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_global_vars',
- 'day_count': 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_cn_day_counts',
- 'area_count': 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_area_counts',
- 'news' : 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_time_line'

Parse the Json and store it to MySQL DB.
