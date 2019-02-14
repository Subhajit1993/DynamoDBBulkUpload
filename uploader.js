let AWS = require('aws-sdk');
AWS.config.loadFromPath('./config.json');

var fs = require('fs');
var parse = require('csv-parse');
var async = require('async');

var dynamodbDocClient = new AWS.DynamoDB({apiVersion: '2012-08-10'});

var csv_filename = "clg.csv";

rs = fs.createReadStream(csv_filename);
parser = parse({
    columns : true,
    delimiter : ','
}, function(err, data) {
    var split_arrays = [], size = 25;

    while (data.length > 0) {

        //split_arrays.push(data.splice(0, size));
        let cur25 = data.splice(0, size)
        let item_data = []

        for (var i = cur25.length - 1; i >= 0; i--) {
            const this_item = {
                "PutRequest" : {
                    "Item": {
                        // your column names here will vary, but you'll need do define the type
                        "clg_name": {
                            "S": cur25[i].clg_name
                        },
                        "clg_icon":{
                            "S":"https://res.cloudinary.com/joelspoyl/image/upload/v1537871969/Group_8_nqwlsa.png"
                        },
                        "clg_image": {
                            "S":"https://res.cloudinary.com/joelspoyl/image/upload/v1537871969/Group_8_nqwlsa.png"
                        },
                        "clg_city": {
                            "S": cur25[i].clg_city
                        },
                        "clg_state": {
                            "S": cur25[i].clg_state
                        },
                        "daily_login_points": {
                            "N": "0"
                        },
                        "other_points": {
                            "N": "0"
                        },
                        "ref_points": {
                            "N": "0"
                        },
                        "signup_points": {
                            "N": "0"
                        },

                    }
                }
            };
            item_data.push(this_item)
        }
        split_arrays.push(item_data);
    }
    data_imported = false;
    chunk_no = 1;
    async.each(split_arrays, (item_data, callback) => {
        const params = {
            RequestItems: {
                "CapCollege" : item_data
            }
        }
        dynamodbDocClient.batchWriteItem(params, function(err, res, cap) {
            if (err === null) {
                console.log('Success chunk #' + chunk_no);
                data_imported = true;
            } else {
                console.log(err);
                console.log('Fail chunk #' + chunk_no);
                data_imported = false;
            }
            chunk_no++;
            callback();
        });

    }, () => {
        // run after loops
        console.log('all data imported....');

    });

});
rs.pipe(parser);