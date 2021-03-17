import json
data = {
    "startSn": 1800050001,
    "accessHost": "group-a2.f-qa.igen",
    "accessPort": 10000,
    "parseRestHost": "http://10.42.9.21",
    "parseRestPort": 9121,
    "sensor": [
        [
            {

				"pdk_ruleCode": "0102010505",

				"attribute": {

					"devNum": 1,

					"devType": 5,

					"location": [

						1,

						2,

						4,

						6

					]

				}

			}
        ]
    ]
}
print(json.dumps(data))
