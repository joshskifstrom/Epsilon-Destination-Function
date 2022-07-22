import fetch from "node-fetch";
import btoa from "btoa";
import crypto from "crypto";
import AWS from "aws-sdk";
import moment from "moment";

let events = [
  {
    context: {
      integrations: {
        personas: false,
      },
      library: {
        name: "unknown",
        version: "unknown",
      },
      personas: {
        computation_class: "audience",
        computation_id: "aud_23WpuMgJJInPmfmY9j9ByBrGILP",
        computation_key: "j_o_drive_repeat_buyers__order_again_ads_d0o5o",
        namespace: "spa_7NoLrHoQfgv2NzMpNucoPs",
        space_id: "spa_7NoLrHoQfgv2NzMpNucoPs",
      },
    },
    integrations: {
      All: false,
      Amplitude: true,
      "Google Analytics": true,
      Warehouses: {
        all: false,
      },
    },
    messageId: "personas_2BfsNCasrxLTQRVUwSwK2R0OdNl",
    originalTimestamp: "2022-07-08T19:27:10.634Z",
    receivedAt: "2022-07-08T19:27:21.675Z",
    sentAt: null,
    timestamp: "2022-07-08T19:27:10.634Z",
    traits: {
      email: "klat0031@umn.edu",
      j_o_drive_repeat_buyers__order_again_ads_d0o5o: true,
    },
    type: "identify",
    userId: "1007382276",
    writeKey: "ufIlKcIAWxTyIUxAxmB8S6celZcDLVIl",
  },
  {
    context: {
      integrations: {
        personas: false,
      },
      library: {
        name: "unknown",
        version: "unknown",
      },
      personas: {
        computation_class: "audience",
        computation_id: "aud_23WpuMgJJInPmfmY9j9ByBrGILP",
        computation_key: "j_o_drive_repeat_buyers__order_again_ads_d0o5o",
        namespace: "spa_7NoLrHoQfgv2NzMpNucoPs",
        space_id: "spa_7NoLrHoQfgv2NzMpNucoPs",
      },
    },
    integrations: {
      All: false,
      Amplitude: true,
      "Google Analytics": true,
      Warehouses: {
        all: false,
      },
    },
    messageId: "personas_2BfsNCasrxLTQRVUwSwK2R0OdNl",
    originalTimestamp: "2022-07-08T19:27:10.634Z",
    receivedAt: "2022-07-08T19:27:21.675Z",
    sentAt: null,
    timestamp: "2022-07-08T19:27:10.634Z",
    traits: {
      email: "aidene1110@hotmail.com",
      j_o_drive_repeat_buyers__order_again_ads_d0o5o: true,
    },
    type: "identify",
    userId: "dc550403-5c4c-43c4-a5a7-9f8fcea4bc09",
    writeKey: "ufIlKcIAWxTyIUxAxmB8S6celZcDLVIl",
  },
];

let logged_event = JSON.stringify(events, null, 2);
//console.log(logged_event);

let settings = {
  s3Bucket: "nike-XXX",
  s3BucketRegion: "us-west-2",
  accessKeyId: "AKIAQ3G6WPXXXX",
  secretAccessKey: "VLD4TEv6deArvoYbZXXXX",
  personasSpaceId: "spa_1bsKVj68gbOu4d0zGtG5pBsMGKw",
  personasProfileApiKey:
    "FmbFl8HXLIX2znZibIo_k6QNCj63DCm0Y5jhwpvEogjlNmuRcZi7xNk38npKpk8X4p_6apxx6OQIQ57tigLAxv8pccyuSSUTXTBCAV1yOj9fGEXlhVkzFta0KPDuTKMIN6FSqHXfnSOOzkHU0LXimQdWN7PhHLRjRfinSbE8GlC-DBa5sQTKtJlQdlysgTA0LjHWd9Xc2VLw0jyCXXXXXX",
};

let records;
let flag = false;
async function onBatch(events, settings) {
  //S3 connection
  const ID = settings.accessKeyId;
  const SECRET = settings.secretAccessKey;
  const BUCKET = settings.s3Bucket; // setting.bucket
  const REGION = settings.s3BucketRegion; //settings.region;
  // Connect to S3
  var credentials = new AWS.Credentials({
    accessKeyId: ID,
    secretAccessKey: SECRET,
  });
  var config = new AWS.Config({
    credentials: credentials,
    region: REGION,
  });
  AWS.config.update(config);
  var s3 = new AWS.S3();

  for (const event of events) {
    let userId = event.userId;
    let user_email;
    let user_phone;
    let audience_key;
    let audience_key_value;

    let header = [
      "user_email",
      "user_phone",
      "audience_key",
      "audience_key_value",
    ];

    let personas_url = `https://profiles.segment.com/v1/spaces/${settings.personasSpaceId}/collections/users/profiles/user_id:${userId}/external_ids?limit=25`;
    console.log(personas_url);
    let profile_api_request = await fetch(personas_url, {
      headers: {
        Authorization: "Basic " + btoa(settings.personasProfileApiKey + ":"),
        "Content-Type": "application/json",
      },
      method: "get",
    });
    if (profile_api_request.ok) {
      let external_ids_json = await profile_api_request.json();

      //collect email from events object and hash
      user_email = sha256Hash(event.traits.email.toLowerCase()).toUpperCase();

      //collect first phone number from profile api array
      user_phone = external_ids_json.data.find((data) => data.type == "phone");

      //if user doesn't have phone, default to null otherwise hash
      if (user_phone == undefined) user_phone = null;
      else user_phone = sha256Hash(user_phone.toLowerCase()).toUpperCase();

      audience_key = event.context.personas.computation_key; //collect audience key name
      audience_key_value = event.traits[audience_key]; //use audience key name to collect audience boolean value

      //csv file headers email,phone,audience_key,audience_key_value /r
      if (!flag) {
        //enter first record
        records = `${user_email},${user_phone},${audience_key},${audience_key_value},\r`;
        flag = true;
      } //append subsequent records
      else
        records += `${user_email},${user_phone},${audience_key},${audience_key_value},\r`;
      console.log("record", records);
    } else {
      if (
        profile_api_request.status == 429 ||
        profile_api_request.status >= 500
      ) {
        console.log(
          "Profile API retryable error. " +
            profile_api_request.status +
            profile_api_request.statusText
        );
        throw new RetryError(
          "Profile API retryable error. " +
            profile_api_request.status +
            profile_api_request.statusText
        );
      } else if (profile_api_request.status == 404) {
        console.log(
          "User not found, userId = " +
            userId +
            ". Error " +
            profile_api_request.status +
            profile_api_request.statusText
        );
        return;
      } else {
        console.log(
          "Profile API non-retryable error. " +
            profile_api_request.status +
            profile_api_request.statusText
        );
        throw new RetryError(
          "Profile API non-retryable error. " +
            profile_api_request.status +
            profile_api_request.statusText
        );
      }
    }
  }

  // Prepare request
  const request = {
    Bucket: BUCKET,
    Key: setMinutes(), //assign file name
    Body: records,
    ContentType: "text/csv;charset=utf-8",
  };

  // Make request
  await s3
    .upload(request)
    .promise()
    .then((data) => {
      console.log(data);
    })
    .catch((err) => {
      throw err;
    });
}

onBatch(events, settings);

/**
 * UTILITY FUNCTIONS
 */
function sha256Hash(value) {
  return crypto
    .createHash("sha256")
    .update(value)
    .digest("hex")
    .toLocaleLowerCase();
}

//assigns file name in date-time -- defaults to 00, 20, 40 minute filename intervals
function setMinutes() {
  let date = moment().format();
  let minutes = JSON.stringify(moment(date, false).get("minutes"));

  if (minutes.length == 1) minutes = "0" + minutes; //account for single digit 0-10 minutes

  date = date.substring(0, 14);
  let digit = minutes.substring(0, 1);
  let unix = Date.now();

  if (digit >= 0 && digit <= 1) return date + "00/" + unix + ".csv";
  else if (digit >= 2 && digit <= 3) return date + "20/" + unix + ".csv";
  else return date + "40/" + unix + ".csv";
}

function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}
