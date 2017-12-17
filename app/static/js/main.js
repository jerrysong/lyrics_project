"use strict";

const BACKEND_END_POINT = "http://50.23.83.252:80/get_top_words_by_artist";

$(function() {
    d3.select("#visual")
      .attr("width", 850)
      .attr("height", 350)
      .append("g")
      .attr("transform", "translate(320,200)");

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
                console.log(JSON.stringify(result));
                showWordCloud(result["top_single_word"])
            },
            error: function(){
                //$("#output-box").html("");
            }
        });
    });
});

function showWordCloud(words) {
    normalize(words);
    d3.layout.cloud().size([800, 300])
             .words(words)
             .rotate(0)
             .fontSize(function(d) { return d.value; })
             .on("end", draw)
             .start();
}

function draw(words) {
    var fill = d3.scale.category20();
    d3.select("#visual")
      .select("g")
      .selectAll("text")
      .data(words)
      .enter().append("text")
      .style("font-size", function(d) { return d.value + "px"; })
      .style("fill", function(d, i) { return fill(i); })
      .attr("transform", function(d) {
          return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
      })
      .text(function(d) { return d.text; });
}

function findMax(words) {
    var hiBound = 0;
    words.forEach(function(word) {
        hiBound = Math.max(hiBound, word.value);
    });
    return hiBound;
}

function normalize(words) {
    var hiBound = findMax(words);
    for (var i=0; i<words.length; i++) {
        words[i].value *= (100 / hiBound);
    }
}
