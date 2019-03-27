This directory contains HTML templates for creating web pages from JSON-encoded forecasts. 

The JSON model data simply replaces the string "<MODEL>" in the forecast template. 

Similarly, an index of all generated forecasts replaces the string "<MODELINDEX>" in the index template. 

HTML files are intended to be generated periodically from JSON forecasts. Once generated, they can be provided to end users via a normal web server. 

Sample JSON data is given below. This is simply assigned to a variable in the HTML page and then formatted for display. 

{
	"source":"rf_test1",
	"sourcelabel":"rf test 1",
	"site":"UGA-C-POA-1-IRR",
	"created":1550736650906,
	"start":1510142400000,
	"stop":1510228800000,
	"columns":[
		{"label":"TIMESTAMP","units":"","longname":"","type":"datetime"},
		{"label":"UGA-C-POA-1-IRR","units":"w/m2","longname":"","type":"number"}
		],
	"rows":[
		["2017-11-08 12:00:00",6.065183055555549],
		["2017-11-08 13:00:00",31.46784888888889],
		["2017-11-08 14:00:00",68.25919000000005],
		["2017-11-08 15:00:00",90.45589760237971],
		["2017-11-08 16:00:00",50.2633881944444],
		["2017-11-08 17:00:00",22.679933869949444],
		["2017-11-08 18:00:00",45.70434916666664],
		["2017-11-08 19:00:00",34.95386999999995],
		["2017-11-08 20:00:00",27.662586944444463],
		["2017-11-08 21:00:00",11.051701111111129],
		["2017-11-08 22:00:00",73.62441263888886],
		["2017-11-08 23:00:00",5.668792307449482],
		["2017-11-09 00:00:00",5.409829861111108],
		["2017-11-09 01:00:00",4.422578055555556],
		["2017-11-09 02:00:00",2.021209444444444],
		["2017-11-09 03:00:00",1.4476183333333374],
		["2017-11-09 04:00:00",0.0],
		["2017-11-09 05:00:00",-0.0036365277777777776],
		["2017-11-09 06:00:00",12.723052361111101],
		["2017-11-09 07:00:00",4.422578055555556],
		["2017-11-09 08:00:00",28.37874833333334],
		["2017-11-09 09:00:00",4.993746944444455],
		["2017-11-09 10:00:00",0.0],
		["2017-11-09 11:00:00",0.03728083333333331],
		["2017-11-09 12:00:00",0.41544999999999954]
		]}
