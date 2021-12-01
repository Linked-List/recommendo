const express = require('express');
const router = express.Router();
const GoogleCustomSearch = require("./services/google-custom-search");
const { KafkaDriver, keywordResMap } = require("./services/kafka");

TOTAL_URLS = 10;

PRODUCER_TOPIC = "urls"

router.get('/', (req, res) => {
    res.render("index");
});
router.get('/search', async (req, res) => {
    console.log(req.query);
    let word = req.query.word; // WARNING: req.query.word is object. not string
    const regex = /^[가-힣a-zA-Z0-9]+$/;
    word = regex.exec(word);

    // Remember keyword - res object pair until tf-idf calculated
    // If the keyword is already in process, just add res object to value array
    // If not, create key-value pair and insert
    if (keywordResMap.has(word)) {
        const resArr = keywordResMap.get(word);
        resArr.push(res);
    } else {
        const resArr = new Array();
        resArr.push(res);
        keywordResMap.set(word[0], resArr);
    }

    // Google Custom Search Engine
    const encodedWord = encodeURIComponent(word);
    let urlCount = 0;
    let items;
    while (urlCount < TOTAL_URLS) { // TODO: 429 에러 안뜨는지 확인
        try {
            items = await GoogleCustomSearch.run(encodedWord, urlCount + 1);
        } catch (error) {
            console.log(error);
            return next(error);
        }
        let urls = GoogleCustomSearch.extractUrls(items);

        // urls array to one string format
        let urlsString = "";
        for (const url of urls)
            urlsString += `${url} `;

        const message = {
            key: word,
            value: urlsString,
        };
        // Send urls to kafka urls topic
        try {
            await KafkaDriver.sendMessage(PRODUCER_TOPIC, message);
        } catch (error) {
            console.log(error);
            return next(error);
        }

        urlCount += urls.length;
    }
});
module.exports = router;