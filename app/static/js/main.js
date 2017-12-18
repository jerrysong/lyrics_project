"use strict";

const BACKEND_END_POINT = "http://50.23.83.252:80/get_top_words_by_artist";
const TOP_SINGLE_WORD = "top_single_word";
const TOP_SINGLE_WORD_TFIDF = "top_single_word_tfidf";
const TOP_TWO_GRAM_WORD = "top_two_gram_word";
const TOP_TWO_GRAM_WORD_TFIDF = "top_two_gram_word_tfidf";
const WORD_COUNT_MODE = "single_word_count_mode";
const WORD_TFIDF_MODE = "single_word_tfidf_mode";
const TWO_GRAM_COUNT_MODE = "two_gram_count_mode";
const TWO_GRAM_TFIDF_MODE = "two_gram_tfidf_mode";

$(function() {
    d3.select("#visual")
      .attr("width", 900)
      .attr("height", 500)
      .append("g")
      .attr("transform", "translate(300,200)");

    var currentMode = WORD_COUNT_MODE;
    var currentData = {};

    $("#search-btn").on("click", function() {
        var artistName = $("#search-input").val();
        var data = {
            artist: artistName
        };

        $.ajax({
            url: BACKEND_END_POINT,
            data: JSON.stringify(data),
            method: "POST",
            dataType: "json",
            contentType: "application/json",
            success: function(result){
                Object.keys(result).forEach(function(key) {
                    normalize(result[key], getFontSize(key));
                });
                currentData = result;

                var words = getDataToShow(currentMode, currentData);
                updateTable(words);
                updateWordCloud(words);
            }
        });
    });

    $("input[type=radio][name=radio]").change(function() {
        currentMode = this.value;
        var words = getDataToShow(currentMode, currentData);
        updateTable(words);
        updateWordCloud(words);
    });
});

function getDataToShow(currentMode, currentData) {
    var words;
    switch (currentMode) {
        case WORD_COUNT_MODE:
            words = currentData[TOP_SINGLE_WORD];
            break;
        case WORD_TFIDF_MODE:
            words = currentData[TOP_SINGLE_WORD_TFIDF];
            break;
        case TWO_GRAM_COUNT_MODE:
            words = currentData[TOP_TWO_GRAM_WORD];
            break;
        case TWO_GRAM_TFIDF_MODE:
            words = currentData[TOP_TWO_GRAM_WORD_TFIDF];
            break;
    }
    if (typeof words == "undefined") {
        words = [];
    }
    return words;
}

function getFontSize(key) {
    switch (key) {
        case TOP_SINGLE_WORD:
            return 95;
        case TOP_SINGLE_WORD_TFIDF:
            return 50;
        case TOP_TWO_GRAM_WORD:
            return 50;
        case TOP_TWO_GRAM_WORD_TFIDF:
            return 40;
    }
}

function updateTable(words) {
    var headers = d3.select("table")
                    .select("thead")
                    .select("tr")
                    .selectAll("th");
    if (words.length == 0) {
        headers.classed("hidden", true);
    } else {
        headers.classed("hidden", false);
    }

    var rows = d3.select("table")
                 .select("tbody")
                 .selectAll("tr")
                 .data(words);

    rows.enter().append("tr")
        .selectAll("td")
        .data(function(d) { return [d.text, d.value]; })
        .enter().append("td")
        .text(function(d) { return d; });

    rows.exit()
        .transition()
        .remove();

    var cells = rows.selectAll("td")
                    .data(function(d) { return [d.text, d.value]; })
                    .text(function(d) { return d; });

    cells.enter()
         .append("td")
         .text(function(d) { return d; });

    cells.exit()
        .transition()
        .remove();
}

function updateWordCloud(words) {
    d3.layout.cloud().size([750, 400])
             .words(words)
             .rotate(0)
             .text(function(d) { return d.text; })
             .fontSize(function(d) { return d.size; })
             .padding(10)
             .on("end", draw)
             .start();
}

function draw(words) {
    var fill = d3.scale.category20();
    var vis = d3.select("#visual")
                .select("g")
                .selectAll("text")
                .data(words);

    vis.enter().append("text")
       .style("font-size", function(d) { return d.size + "px"; })
       .style("font-family", "Impact")
       .style("fill", function(d, i) { return fill(i); })
       .attr("text-anchor", "middle")
       .attr("transform", function(d) {
           return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
       })
       .text(function(d) { return d.text; });

    vis.transition()
       .duration(100)
       .style("font-size", function(d) { return d.size + "px"; })
       .style("font-family", "Impact")
       .style("fill", function(d, i) { return fill(i); })
       .attr("text-anchor", "middle")
       .attr("transform", function(d) {
           return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
       })
       .text(function(d) { return d.text; });;

    vis.exit()
       .transition()
       .duration(100)
       .remove();
}

function findMax(words) {
    var hiBound = 0;
    words.forEach(function(word) {
        hiBound = Math.max(hiBound, word.value);
    });
    return hiBound;
}

function normalize(words, fontSize) {
    var hiBound = findMax(words);
    for (var i=0; i<words.length; i++) {
        words[i]["size"] = words[i].value * (fontSize / hiBound);
    }
}
