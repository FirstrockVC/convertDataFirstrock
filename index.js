const express = require('express');
const bodyParser = require('body-parser');
const csv = require('csvtojson');
const alasql = require('alasql');
const Moment = require('moment');
const _ = require('lodash');
const csv_out = require('express-csv');
const momentRange = require('moment-range');
const moment = momentRange.extendMoment(Moment);

alasql.fn.moment = moment;
const app = express();
let dataFile = [];

/**
 * Wrapper method to convert CSV into JSON and format dates
 * @param filename
 * @returns {Promise}
 */
const csv2json = (data) => {
  return new Promise((success, reject) => {
    // Transform CSV into JSON
    csv()
      .fromString(data)
      .on('json',(jsonObj)=>{
        dataFile.push({
          distinct_id: jsonObj.distinct_id,
          name: jsonObj.name, 
          time: jsonObj.time,         
          date: moment(Number(jsonObj.time)).format('MM/DD/YYYY')
        });
      })
      .on('done',(error)=>{
        if(error) {
          reject(error);
        } else {
          success(dataFile);
        }
      })
  });
};

// Body parser configuration
app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({limit: '50mb', extended: true}));

// Add headers
app.use((req, res, next) => {
      // Website you wish to allow to connect
      res.setHeader('Access-Control-Allow-Origin', '*');
      // Request methods you wish to allow
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
      // Request headers you wish to allow
      res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
      // Set to true if you need the website to include cookies in the requests sent
      // to the API (e.g. in case you use sessions)
      res.setHeader('Access-Control-Allow-Credentials', true);
      // Pass to next layer of middleware
      next();
  });

// Initial API Endpoint
app.get('/', (req, res) => {
  res.json({ api: 'V1.0', description: 'convert API'});
});

app.post('/uploadFile', (req, res) => {
  const body =  req.body;
  csv2json(body.data)
    .then((data) => {
        const dataEvents = alasql('SELECT DISTINCT name from ?', [data]); 
        res.send(dataEvents);
    })
    .catch((error) => {
      res.status(500).send('Something broke!');
    });
});

app.get('/convert', (req, res) => {
  res.send(dataFile);
});

app.listen(process.env.PORT || 3000, ()=> {
  console.log('Example app listening on port 3000!');
});