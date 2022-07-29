async function onBatch(events, settings) {
	let records;
	let flag = false;
	//S3 connection
	const ID = settings.accessKeyId;
	const SECRET = settings.secretAccessKey;
	const BUCKET = settings.s3Bucket;
	const REGION = settings.s3BucketRegion;
	// Connect to S3
	var credentials = new AWS.Credentials({
		accessKeyId: ID,
		secretAccessKey: SECRET
	});
	var config = new AWS.Config({
		credentials: credentials,
		region: REGION
	});
	AWS.config.update(config);
	var s3 = new AWS.S3();

	//iterate through onBatch events
	for (const event of events) {
		let userId = event.userId;
		let user_email;
		let user_phone;
		let audience_key;
		let audience_key_value;

		let header = [
			'user_email',
			'user_phone',
			'audience_key',
			'audience_key_value'
		];

		//collect phone from profile api
		let personas_url = `https://profiles.segment.com/v1/spaces/${settings.personasSpaceId}/collections/users/profiles/user_id:${userId}/external_ids?limit=25`;
		let profile_api_request = await fetch(personas_url, {
			headers: {
				Authorization: 'Basic ' + btoa(settings.personasProfileApiKey + ':'),
				'Content-Type': 'application/json'
			},
			method: 'get'
		});
		if (profile_api_request.ok) {
			let external_ids_json = await profile_api_request.json();

			//collect email from events object and hash
			//if user doesn't have phone, default to null otherwise hash
			user_email = event.traits.email;
			if (user_email == undefined) user_email = null;
			else
				user_email = sha256Hash(event.traits.email.toLowerCase()).toUpperCase();

			//collect first phone number from profile api array
			user_phone = external_ids_json.data.find(
				data => data.type == 'phone' || data.type == 'phone_number'
			);

			//if user doesn't have phone, default to null otherwise hash
			if (user_phone == undefined) user_phone = null;
			else user_phone = sha256Hash(user_phone.id.toLowerCase()).toUpperCase();

			audience_key = event.context.personas.computation_key; //collect audience key name
			audience_key_value = event.traits[audience_key]; //use audience key name to collect audience boolean value

			//csv file headers email,phone,audience_key,audience_key_value \r
			if (!flag) {
				//enter first record
				records = `${user_email},${user_phone},${audience_key},${audience_key_value},\r`;
				flag = true;
			} //append subsequent records
			else
				records += `${user_email},${user_phone},${audience_key},${audience_key_value},\r`;
		} else {
			if (
				profile_api_request.status == 429 ||
				profile_api_request.status >= 500
			) {
				console.log(
					'Profile API retryable error. ' +
						profile_api_request.status +
						profile_api_request.statusText
				);
				throw new RetryError(
					'Profile API retryable error. ' +
						profile_api_request.status +
						profile_api_request.statusText
				);
			} else if (profile_api_request.status == 404) {
				console.log(
					'User not found, userId = ' +
						userId +
						'. Error ' +
						profile_api_request.status +
						profile_api_request.statusText
				);
				return;
			} else {
				console.log(
					'Profile API non-retryable error. ' +
						profile_api_request.status +
						profile_api_request.statusText
				);
				throw new RetryError(
					'Profile API non-retryable error. ' +
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
		ContentType: 'text/csv;charset=utf-8'
	};

	// Make request
	await s3
		.upload(request)
		.promise()
		.then(data => {
			console.log(data);
		})
		.catch(err => {
			throw err;
		});
}

/**
 * UTILITY FUNCTIONS
 */
function sha256Hash(value) {
	return crypto
		.createHash('sha256')
		.update(value)
		.digest('hex')
		.toLocaleLowerCase();
}

//assigns directory name in 20 minutes chunks 'yyyy-mm-ddThh:mm/' - filename in unix time '1658520411394.csv'
function setMinutes() {
	let date = moment().format();
	let minutes = JSON.stringify(moment(date, false).get('minutes'));

	if (minutes.length == 1) minutes = '0' + minutes; //account for single digit 0-10 minutes

	date = date.substring(0, 14);
	let digit = minutes.substring(0, 1);
	let unix = Date.now();

	if (digit >= 0 && digit <= 1) return date + '00/' + unix + '.csv';
	else if (digit >= 2 && digit <= 3) return date + '20/' + unix + '.csv';
	else return date + '40/' + unix + '.csv';
}
