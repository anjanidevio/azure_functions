var express = require('express');
var Webtask = require('webtask-tools');
var bodyParser = require('body-parser');
var app = express();
var sql = require('mssql');
var weather = require('weather-js');
var dateFormat = require('dateformat');
var clientFromConnectionString = require('azure-iot-device-amqp').clientFromConnectionString;
var Message = require('azure-iot-device').Message;
var TIME_INCREMENT = 0;

var TIME_FREQUENCY_INTERVAL = 60; // second get from app setting process.env['Application_setting']
var IOT_CONNECTION_KEY = 'HostName=node-iot.azure-devices.net;DeviceId=node-device;SharedAccessKey=G02P7emqYVpg/R/vBq0gDM2TNAnur4IC+PP69LtxC2g=';

app.use(bodyParser.json());
var CONFIG = {
    server: 'streaming-data.database.windows.net',
    database: 'streaming-data',
    user: 'rajesh',
    password: 'asd123!@#',
    port: 1433,
    options: {
        encrypt: true // Use this if you're on Windows Azure
    }
};

var request = require('request');

app.get('/api/sensor', function(req, res) {
  generateToken().then(function(sensorToken){
    getSensorData(sensorToken).then(function(sensorData) {
      res.json({sensorData});
    })
  });
});

app.get('/api/layout', function(req, res) {
    var query = "select * from layout";
    sqlSelect(query, function(response) {
        res.status(200).send(response);
    });
});

app.get('/api/layout/:type', function(req, res) {
    var type = req.params.type;
    //type = type.charAt(0).toUpperCase() + type.slice(1)
    var output = {};
    var query = "select * from layout where layoutType = '" + type + "'";
    sqlSelect(query, function(layoutResponse) {
        var query = "select top " + layoutResponse.data[0].powerscoutSize + " * from AssetDetails";
        sqlSelect(query, function(PowerscoutResponse) {
            output.powerscout = PowerscoutResponse;
            var query = "select top " + layoutResponse.data[0].sensorSize + " * from sensors";
            sqlSelect(query, function(sensorsResponse) {
                output.sensors = sensorsResponse;
                res.status(200).send(output);
            })
        });
    });
});

app.post('/api/simulate/:range', function(req, res) {

    if (Object.keys(req.body).length === 0) {
        res.status(200).send({
            "Success": false,
            "data": "Invalid JSON data input"
        });
    }
    TIME_INCREMENT = 0;
    validate_Range_Interval(req.params.range, TIME_FREQUENCY_INTERVAL).then(function(validateRes) {
        if (validateRes.valid) {
            generateMultipleIOTData(req, validateRes).then(function(IOTDataList) {
                console.log("Exact IOT DATA-", JSON.stringify(IOTDataList));
                IOTDataList.forEach(function(value, key, myArray) {
                    //console.log(value);
                    //setTimeout(sendDataToIOT(value), 5000);
                    //sendDataToIOT(value);
                });
            });
            res.status(200).send({
                "Success": true
            });
        } else {
            res.status(200).send({
                "Success": false,
                "data": "Invalid Range input"
            });
        }
    });
});

function generateToken() {
  return new Promise((_resolve, _reject) => {
    try {
        var client_id = '8d60ff5b-af3e-4b93-ba8b-befa02ec46f8';
        var client_secret = 'fa99db50-fb2b-4111-a288-b8fe8c2843db';
        var client_code = '0a56eb59-4b15-4ac1-9062-5ea664cbb443';
        var tokenQuery = '?client_id=' + client_id + '&client_secret=' + client_secret + '&code=' + client_code;
         
         var reqToken = {
           uri: 'https://my.wirelesstag.net/oauth2/access_token.aspx' + tokenQuery,
           method: 'POST',
           headers: {
             'Content-Type': 'application/x-www-form-urlencoded',
             'Accept': 'application/json'
           }
         }
       request(reqToken, function(error, response, body){
          _resolve(JSON.parse(body));
       });   
    } catch(err){
      _reject(err)
    }
  });
}

function getSensorData(auth_token) {
  return new Promise((_resolve, _reject) => {
    try {
       var req = {
          uri: 'https://my.wirelesstag.net/ethClient.asmx/GetTagList',
          method: 'POST',
          json: {"uuid": "0f32cb87-eb86-4a60-9e66-e0cfd661cba1"},
          headers: {
              Authorization: 'Bearer ' + auth_token.access_token,
              'Content-Type': 'application/json'
                    }
          };
        request(req,callback);
        function callback(error, response, body) {
          console.log("Sensor Row Data-",body);
          _resolve(body);
        }    
    } catch(err){
      _reject(err);
    }
  });
}

function generateMultipleIOTData(req, validateData) {
    return new Promise((_resolve, _reject) => {
        try {
            var IOTDATAObj = [];
            for (var k = 0; k < validateData.rangeInterval; k++) {
                generateSingleIOTData(req).then(function(IOTData) {
                    IOTDATAObj.push(IOTData);
                    if (IOTDATAObj.length === validateData.rangeInterval) {
                        _resolve(IOTDATAObj);
                    }
                });
            }
        } catch (err) {
            _reject({
                'error': 'server error'
            });
        }
    });
}

function generateSingleIOTData(req) {
    return new Promise((_resolve, _reject) => {
        try {
            var subIOTObj = {};
            var powerList = [];
            var sensorsList = [];

            getWeatherData(req.body.weather.ZipCode).then(function(weatherOutput) {
                var dateTime = getDateTime();
                req.body.powerscout.forEach(function(data, key) {
                      powerList.push({
                        "PowerScout": data.PowerScout,
                        "TimeStamp": dateTime,
                        "Amps L1": "18",
                        "Amps L2": "18",
                        "Amps L3": "19",
                        "Amps System Avg": "16",
                        "Daily Electric Cost": "0",
                        "Daily kWh System": "112.233333333333",
                        "Monthly Electric Cost": "39",
                        "Monthly kWh System": "200",
                        "ReadingTime": "90",
                        "Rolling Hourly kWh System": "17.3333333333333",
                        "Volts L1 to Neutral": "43",
                        "Volts L2 to Neutral": "44",
                        "Volts L3 to Neutral": "41",
                        "kW L1": "40",
                        "kW L2": "41",
                        "kW L3": "42",
                        "kW System": "39",
                        "Breaker Details": data['Breaker Details'] || data.BreakerDetails,
                        "Breaker Label": data['Breaker Label'] || data.BreakerLabel,
                        "Building": data.Building,
                        "Modbus Base Address": data['Modbus Base Address'] || data.ModbusBaseAddress,
                        "Serial Number": data['Serial Number'] || data.SerialNumber,
                        "Type": data.Type,
                        "Rated Amperage": data['RatedAmperage'] || data.RatedAmperage
                    });            
                  
                });

                req.body.sensors.forEach(function(data, key) {
                    sensorsList.push({
                        "Wireless Tag Template": data.SensorId,
                        "TimeStamp": dateTime,
                        "Brightness": "3.8702929019928",
                        "Humidity": "29.0189208984375",
                        "Name": data.SensorId,
                        "Temperature": "71.8601989746094"
                    });
                });

                subIOTObj.powerscout = powerList;
                subIOTObj.sensors = sensorsList;
                subIOTObj.weather = weatherOutput;
                subIOTObj.weather.TimeStamp = dateTime;
                _resolve(subIOTObj);
            });
        } catch (err) {
            _reject({
                'error': 'server error'
            });
        }
    });
}

function validate_Range_Interval(range, interval) {
    return new Promise((_resolve, _reject) => {
        try {
            var rangeList = ['daily', 'weekly', 'monthly', 'quarterly', 'halfyearly', 'yearly', 'default'];
            rangeList.forEach(function(element) {
                if (element === range) {
                    _resolve({
                        'valid': true,
                        'rangeInterval': generateRangeInterval(range, interval)
                    });
                }
            });
            _resolve({
                'valid': false
            });
        } catch (err) {
            _reject({
                'valid': false
            });
        }
    });
}

function generateRangeInterval(range, interval) {
    switch (range) {
        case "daily":
            return (24 * 60 * 60) / interval; // 1 Day
        case "weekly":
            return (7 * 24 * 60 * 60) / interval; // 1 weekly 
        case "monthly":
            return (30 * 24 * 60 * 60) / interval; // 1 Monthly
        case "quarterly":
            return (90 * 24 * 60 * 60) / interval;
        case "halfyearly":
            return (180 * 24 * 60 * 60) / interval;
        case "yearly":
            return (360 * 24 * 60 * 60) / interval;
        case "default":
            return 10
    }
}

function sendDataToIOT(telmentryData) {

    var client = clientFromConnectionString(IOT_CONNECTION_KEY);

    function printResultFor(op) {
        return function printResult(err, res) {
            if (err) console.log(op + ' error: ' + err.toString());
            if (res) console.log(op + ' status: ' + res.constructor.name);
        };
    }
    var connectCallback = function(err) {
        if (err) {
            console.log('Could not connect to IoT Hub: ' + err);
        } else {
            console.log('Client connected to IoT Hub');
            client.on('message', function(msg) {
                client.complete(msg, printResultFor('completed'));
            });
            var message = new Message(JSON.stringify(telmentryData));
            console.log("Telemetry sent: " + message.getData());
            client.sendEvent(message, printResultFor('send'));
        }
    };
    client.open(connectCallback);
}

function getWeatherData(location) { // 'San Francisco, CA'
    return new Promise((_resolve, _reject) => {
        try {
            weather.find({
                search: location,
                degreeType: 'F'
            }, function(err, result) {
                if (err)
                    console.log(err);
                var windDirection_list = ['North', 'North-Northeast', 'East-Northeast', 'East'];
                var data = {
                    "Weather": result[0].current.skytext, //"haze",
                    "Pressure": "1013",
                    "Relative Humidity": result[0].current.humidity,
                    "Temperature": result[0].current.temperature,
                    "Visibility": result[0].current.winddisplay, //"5000",
                    "Weather.Weather": result[0].current.skytext, //"haze",
                    "Wind Direction": windDirection_list[Math.floor(Math.random() * windDirection_list.length)], //"NORTH-EAST"
                    "Wind Speed": result[0].current.windspeed
                }
                _resolve(data);
            });
        } catch (err) {
            _reject(err);
        }
    });
}

function sqlSelect(query, callback) {
    var dbConn = new sql.ConnectionPool(CONFIG);
    dbConn.connect().then(function() {
        var request = new sql.Request(dbConn);
        request.query(query).then(function(response) {
            dbConn.close();
            callback({
                "Success": true,
                "data": response.recordset
            });
        }).catch(function(err) {
            dbConn.close();
            callback({
                "Success": false,
                "data": "Server error"
            });
        });
    }).catch(function(err) {
        callback({
            "Success": false,
            "data": "Server error"
        });
    });
}

function getDateTime() {
    var date = new Date(dateFormat(new Date(), "yyyy-mm-dd HH:MM:ss"));
    TIME_INCREMENT = TIME_INCREMENT + TIME_FREQUENCY_INTERVAL;
    date.setSeconds(date.getSeconds() + (TIME_INCREMENT));
    var actualTime = dateFormat(new Date(), "yyyy-mm-dd HH") + ":" + date.getMinutes() + ":0" + date.getSeconds() + "." + dateFormat(new Date(), "l");
    return actualTime;
}

module.exports = Webtask.fromExpress(app);


/*

{
  "powerscout" : [
   {
      "PowerScout": "P371602012",
      "BreakerDetails": "New(2012) 3rd floor panel",
      "BreakerLabel": "PP12 - 3rd PI Electric Rm",
      "Building": "Building-12",
      "ModbusBaseAddress": "2",
      "SerialNumber": "P371602012",
      "Type": "PowerScout 2012",
      "RatedAmperage": "500"
   },
   {
      "PowerScout": "P371602013",
      "BreakerDetails": "New(2013) 3rd floor panel",
      "BreakerLabel": "PP12 - 3rd PI Electric Rm",
      "Building": "Building-13",
      "ModbusBaseAddress": "2",
      "SerialNumber": "P371602013",
      "Type": "PowerScout 2013",
      "RatedAmperage": "500"
   }
],
  "sensors": [
   {
      "SensorId": "Pro Sensor 1",
      "BreakerDetails": "New(2012) 3rd floor panel",
      "BreakerLabel": "PP12 - 3rd PI Electric Rm",
      "Building": "Building-12",
      "ModbusBaseAddress": "2",
      "SerialNumber": "P371602012",
      "Type": "Sensor 2012",
      "RatedAmperage": "500"
   },
   {
      "SensorId": "Pro Sensor 4",
      "BreakerDetails": "New(2013) 3rd floor panel",
      "BreakerLabel": "PP12 - 3rd PI Electric Rm",
      "Building": "Building-13",
      "ModbusBaseAddress": "2",
      "SerialNumber": "P371602013",
      "Type": "Sensor 2013",
      "RatedAmperage": "500"
   }
],
  "weather": {"ZipCode": "500081"}
}


*/