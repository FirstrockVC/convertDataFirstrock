const csv = require('csvtojson');
const express = require('express');
const alasql = require('alasql');
let dataFile = [];
const Moment = require('moment');
const _ = require('lodash');
const csv_out = require('express-csv');
const momentRange = require('moment-range');
const moment = momentRange.extendMoment(Moment);
const app = express();

const csv2json = (data,type) => {
  dataFile = [];  
  return new Promise((success, reject) => {
    // Transform CSV into JSON
    csv()
      .fromFile('./data.csv')
      .on('json',(jsonObj)=>{
        dataFile.push({
            user: jsonObj.user,         
            time: moment(Number(jsonObj.unix_time * 1000)).format('MM/DD/YYYY hh:mm:ss A'),
            unix: jsonObj.unix_time
          });
      })
      .on('done',(error)=>{
      })
  });
};

app.get('/', (req, res) => {
    csv2json();
    dataFile = [];  
    datafilter = [];
    let datausers = [];
    let report = [];
    return new Promise((success, reject) => {
      csv()
        .fromFile('./users.csv')
        .on('json',(jsonObj)=>{
            datausers.push(jsonObj);
        })
        .on('done',(error)=>{
            _.forEach(datausers, (value) => {
                const joder = _.filter(dataFile,function(o) { return o.unix !== "" && o.user === value.top_users; });
                const data = _.orderBy(joder, ['unix'], ['asc']);
                if (data[0]){
                    const object = data[0];
                    datafilter.push({time: object.time, user: object.user, unix: object.unix });
                }
            });
            const jo = _.orderBy(datafilter, ['unix'], ['asc']);
            console.log(datafilter.length)
            res.json(jo);
        })
    });
});

app.listen(process.env.PORT || 3000, ()=> {
    console.log('Example app listening on port 3000!');
});