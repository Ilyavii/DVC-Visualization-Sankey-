import yaml
import pandas as pd
import plotly.graph_objects as go
import config
from parsing import *

pipelines = yaml.full_load(open(config.path_dvc, "r").read())
df_pipelines = df_from_dict(pipelines)

# ==============================================
# dataframe = df_pipelines
#	key			value				 stage
# 1  cmd		python file.py		  stage_1
# 2  params		datetime			  stage_1
# 3  outs		file.csv			  stage_1
# 4  deps		text				  stage_2
# ==============================================

# for convenience, we create a dataframe
df_dict = df_split_into_columns(df_pipelines)

# ==============================================
# dataframe = df_dict
#	cmd			  params	  outs	   deps		stage
# 1  python file.py   datetime	file.csv   text	   stage_1
# 1  python file_2.py datetime	file_2.csv text	   stage_2
# ==============================================

# ===============
# Making connections
# ===============

df_pipelines['from'] = 0
input_files = df_pipelines[~df_pipelines['label'].isin(df_dict['outs'].tolist())]['label'].tolist()
last_output_files = df_pipelines[~df_pipelines['label'].isin(df_dict['deps'].tolist())]['label'].tolist()
df_pipelines.loc[(df_pipelines['label'].isin(input_files)), 'from'] = "input" # first file
df_pipelines.loc[(df_pipelines['label'].isin(last_output_files)), 'from'] = "end_file" # end file
df_pipelines['from'] = df_pipelines.apply(lambda x: finde_stage(df_dict, x['label'], x['from'], x['key'], x['stage']), axis=1)

# ===============
# list_label
# ===============

list_label = df_pipelines[~df_pipelines['from'].isin(['script']) & ~df_pipelines['key'].isin(['desc', 'cmd'])]['label'].tolist()
list_label.extend(df_pipelines['stage'].tolist())
list_stage = set(df_pipelines['stage'].tolist())
list_label = set(list_label)
dict_label = dict(zip(list_label, range(len(list_label))))

df_files_input = df_pipelines[~df_pipelines['key'].isin(['desc', 'cmd']) & ~df_pipelines['from'].isin([ 'script'])]

df_files_input['from_num'] = df_files_input.apply(lambda x: get_from_number(x, dict_label), axis=1)
df_files_input['to_num'] = df_files_input.apply(lambda x: get_to_number(x, dict_label), axis=1)
df_files_input['value'] = 3 # used as a stub, change as you like

# ===============
# Definition of colors
# ===============

list_color = []
for value in list(dict_label.keys()):
	if (value[-1] != '/'):
		value = value.split('.')[-1]
	else:
		value = '/'
	if (value in list(config.color_file.keys())):
		list_color.append(config.color_file[value])
	elif (value in list_stage):
		list_color.append(config.color_file['stage_color'])
	else:
		list_color.append(config.color_file['else'])

# ===============
# visualization
# ===============

fig = go.Figure(data=[go.Sankey(
	node = dict(
	  pad = 15,
	  thickness = 20,
	  line = dict(color = "black", width = 0.5),
	  label = find_name_file(list(dict_label.keys())),
	  color = list_color
	),
	link = dict(
	  source = df_files_input['from_num'], # indices correspond to labels, eg A1, A2, A1, B1, ...
	  target = df_files_input['to_num'],
	  value = df_files_input['value'],
	  label =  df_files_input['label']
  ))])

fig.update_layout(title_text="Basic Sankey Diagram", font_size=15)
#fig.show()
fig.write_html(config.path_html, auto_open=True) # save file
