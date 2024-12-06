import pandas as pd
from pyecharts.charts import Bar, Line, Geo, Page
from pyecharts import options as opts
from pyecharts.globals import ThemeType, ChartType

# 读取整个文件并分割数据
file_path = 'output/part-r-00000'
age_distribution = []
brand_sales = []
date_sales = []
local_sales = []
time_sales = []

with open(file_path, 'r') as file:
    current_section = None
    for line in file:
        if line.startswith('age,count'):
            current_section = 'age'
            age_distribution.append(line.strip().split(','))
            continue
        elif line.startswith('brand,sales'):
            current_section = 'brand'
            brand_sales.append(line.strip().split(','))
            continue
        elif line.startswith('day_type,sales'):
            current_section = 'date'
            date_sales.append(line.strip().split(','))
            continue
        elif line.startswith('local,sales'):
            current_section = 'local'
            local_sales.append(line.strip().split(','))
            continue
        elif line.startswith('hour,sales'):
            current_section = 'time'
            time_sales.append(line.strip().split(','))
            continue

        if current_section == 'age':
            age_distribution.append(line.strip().split(','))
        elif current_section == 'brand':
            brand_sales.append(line.strip().split(','))
        elif current_section == 'date':
            date_sales.append(line.strip().split(','))
        elif current_section == 'local':
            local_sales.append(line.strip().split(','))
        elif current_section == 'time':
            time_sales.append(line.strip().split(','))

# 转换为DataFrame
age_distribution = pd.DataFrame(age_distribution[1:], columns=age_distribution[0])
brand_sales = pd.DataFrame(brand_sales[1:], columns=brand_sales[0])
date_sales = pd.DataFrame(date_sales[1:], columns=date_sales[0])
local_sales = pd.DataFrame(local_sales[1:], columns=local_sales[0])
time_sales = pd.DataFrame(time_sales[1:], columns=time_sales[0])

# 转换数据类型
age_distribution['age'] = age_distribution['age'].astype(int)
age_distribution['count'] = age_distribution['count'].astype(int)
brand_sales['sales'] = brand_sales['sales'].astype(float)
date_sales['sales'] = date_sales['sales'].astype(float)
local_sales['sales'] = local_sales['sales'].astype(float)
time_sales['hour'] = time_sales['hour'].astype(int)
time_sales['sales'] = time_sales['sales'].astype(float)

# 创建图表
# 年龄分布柱状图
age_bar = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.DARK))
    .add_xaxis(age_distribution['age'].tolist())
    .add_yaxis("Count", age_distribution['count'].tolist())
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各年龄消费人数统计", title_textstyle_opts=opts.TextStyleOpts(color="#ffffff")),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#ffffff"))
    )
)

# 品牌销售额柱状图
brand_bar = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.DARK))
    .add_xaxis(brand_sales['brand'].tolist())
    .add_yaxis("Sales", brand_sales['sales'].tolist())
    .set_global_opts(
        title_opts=opts.TitleOpts(title="各品牌销售额", title_textstyle_opts=opts.TextStyleOpts(color="#ffffff")),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#ffffff"))
    )
)

# 不同日期类型的销售额柱状图
date_bar = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.DARK))
    .add_xaxis(date_sales['day_type'].tolist())
    .add_yaxis("Sales", date_sales['sales'].tolist())
    .set_global_opts(
        title_opts=opts.TitleOpts(title="不同日期类型的销售额", title_textstyle_opts=opts.TextStyleOpts(color="#ffffff")),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#ffffff"))
    )
)

# 各地区消费额地图
local_geo = (
    Geo(init_opts=opts.InitOpts(theme=ThemeType.DARK))
    .add_schema(maptype="china")
    .add(
        "Sales",
        [list(z) for z in zip(local_sales['local'].tolist(), local_sales['sales'].tolist())],
        type_=ChartType.HEATMAP,
    )
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(
        visualmap_opts=opts.VisualMapOpts(),
        title_opts=opts.TitleOpts(title="各地区消费额", title_textstyle_opts=opts.TextStyleOpts(color="#ffffff")),
        legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#ffffff"))
    )
)

# 不同时间段的销售额折线图
time_line = (
    Line(init_opts=opts.InitOpts(theme=ThemeType.DARK))
    .add_xaxis(time_sales['hour'].tolist())
    .add_yaxis("Sales", time_sales['sales'].tolist())
    .set_global_opts(
        title_opts=opts.TitleOpts(title="不同时间段的销售额", title_textstyle_opts=opts.TextStyleOpts(color="#ffffff")),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(color="#ffffff")),
        legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#ffffff"))
    )
)

# 创建看板
page = Page(layout=Page.DraggablePageLayout)
page.add(age_bar, brand_bar, date_bar, local_geo, time_line)
page.render('dashboard.html')

# 添加标题到看板
with open('dashboard.html', 'r') as file:
    content = file.read()

content = content.replace('<body>', '<body><h1 style="text-align:center;">手机销售数据分析看板</h1>')

with open('dashboard.html', 'w') as file:
    file.write(content)