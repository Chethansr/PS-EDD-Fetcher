const fs = require('fs');
const csv = require('csv-parser');
const request = require("request");
const sleep = require('sleep')
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const csvFilePath = './BrandWisePincodeTimelines.csv'
const eddApi = 'https://eddapi.narvar.com/lululemon/shipping/edd?carrier=delhivery&origin_zip=560001&dest_country_code=IN&carrier_service=GR&origin_country_code=IN&dest_zip='
var cnt = 0;

const csvWriter = createCsvWriter({
  path: './result.csv',
  header: [
    { id: 'dzip', title: 'dzip' },
    { id: 'edd', title: 'EDD' }
  ],
  append: true
});

var baseRequest = request.defaults({
  pool: { maxSockets: 100 }
})

fs.createReadStream(csvFilePath)
  .pipe(csv({ separator: ',' }))
  .on('data', async function(data) {
    try {
      var options = {
        url: eddApi + data.Pincode,
      };

      if (cnt++ >= 1000) {
        cnt = 0;
        sleep.msleep(500)
      }

      baseRequest(options, function(error, response, body) {
        sleep.msleep(100)
        var dzip = data.Pincode
        if (!error && response.statusCode == 200) {
          var info = JSON.parse(body);
          //console.log(dzip + '  ' + info.tracking_info.edd_begin)

          const records = [
            { dzip: dzip, edd: info.tracking_info.edd_begin }
          ];

          csvWriter.writeRecords(records) // returns a promise
            .then(() => {
              console.log('...Done');
            });
        }

      });

    } catch (err) {
      console.log(err)
    }
  })
  .on('end', function() {
    console.log('end')
  });