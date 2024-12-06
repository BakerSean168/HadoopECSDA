import pandas as pd
from pyecharts.charts import Bar, Line, Geo, Page
from pyecharts import options as opts
from pyecharts.globals import ThemeType, ChartType

page.save_resize_html("test.html",
                       cfg_file="chart_config .json",
                       dest="my_test.html")