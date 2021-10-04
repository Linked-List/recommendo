// async function onSearchButtonClicked(event) {
//     event.preventDefault();

//     const searchTermInput = document.querySelector(".js-searchTerm");
//     const searchTerm = searchTermInput.value;

//     const url = `/search?word=${searchTerm}`;
//     const method = "get";
//     console.log(`${searchTerm}`);
//     let response;
//     try {
//         response = await axios({
//             url,
//             method,
//         });
//     } catch(error) {
//         console.log(error);
//     }
//     console.log(response);
//     //let result = response.data.item.relatedKeywords;

//     //result = result.split(" ");

//     //showResult(result, startTime);
// }

function init() {
    // button event
    const searchButton = document.querySelector(".js-searchButton");
    searchButton.addEventListener("click", onSearchButtonClicked);
}

init();
