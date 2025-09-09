Payload to run the API in local

{
  "states": [
    "CT",
    "NJ",
    "NY"
  ],
  "min_mag": 2.5,
  "air_lat": 0,
  "air_lon": 0,
  "pull_earthquakes": false,
  "pull_nws": true,
  "pull_nhc": false,
  "pull_firms": false,
  "pull_airnow": false
}

Please note - to run in local in app.py, comment line 242 and 243 and uncomment 245, and do revert it for running in cloud run with scheduler