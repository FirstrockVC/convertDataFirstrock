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
const jsonData;

/**
 * Wrapper method to convert CSV into JSON and format dates
 * @param filename
 * @returns {Promise}
 */
const csv2json = (data) => {
  return new Promise((success, reject) => {
    let csv_data = [];
    // Transform CSV into JSON
    csv()
      .fromString(data)
      .on('json',(jsonObj)=>{
        csv_data.push({
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
          success(csv_data);
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
  const body =  req.body;
  csv2json(body.data)
    .then((data) => {
        const dataEvents = alasql('SELECT DISTINCT name from ? WHERE', [data]); 
        res.send(dataEvents);
    })
    .catch((error) => {
      res.status(500).send('Something broke!');
    });
});

app.get('/convert', (req, res) => {
  const body =  req.body;
  csv2json(body.data)
    .then((data) => {
        const minDate = alasql('select date from ? WHERE name="Focused 10 Minutes" ORDER BY time ASC limit 1', [data]);
        const maxDate = alasql('select date from ? WHERE name="Focused 10 Minutes" ORDER BY time DESC limit 1', [data]);
        const range = moment.range(moment(minDate[0].date).format('MM/DD/YYYY'), moment(maxDate[0].date).format('MM/DD/YYYY')); 
        const datafinal = [];
        let cumulative = 0;
        let day = 1;
        const dataFilterEvent = alasql('SELECT DISTINCT distinct_id from ? WHERE name="Focused 10 Minutes"', [data]); 
        _.forEach(dataFilterEvent, (event, key) => {
            const user = event.distinct_id;
            cumulative = 0;
            day = 1;
            const resultConvert = alasql('SELECT COUNT(time) * 10 AS cumulative,distinct_id,date from ? WHERE name="Focused 10 Minutes" AND distinct_id= "'+ event.distinct_id + '" GROUP BY date, distinct_id ORDER BY date', [data]); 
            for (let object of resultConvert) {                
              cumulative += object.cumulative; 
                datafinal.push({
                  'cohort_person': user, 
                  'activity_day': 'Day' + day++,
                  'cumulative': cumulative,
                  'unic': object.cumulative,
                  'date': object.date
                  });
            }
        });
        res.csv(datafinal);
    })
    .catch((error) => {
        console.log(error);
      res.status(500).send('Something broke!');
    })
});

app.listen(process.env.PORT || 3000, ()=> {
  console.log('Example app listening on port 3000!');
});