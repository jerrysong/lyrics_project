"use strict";

const BACKEND_END_POINT = "http://50.23.83.252:80/get_top_words_by_artist";

$(function() {
    d3.select("#visual")
      .attr("width", 900)
      .attr("height", 500)
      .append("g")
      .attr("transform", "translate(300,200)");

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
                var words = result["top_single_word"];
                normalize(words);
                updateTable(words)
                updateWordCloud(words);
            }
        });
    });
});

function updateTable(words) {
    var headers = d3.select("table")
                    .select("thead")
                    .select("tr")
                    .selectAll("th");
    if (words.length > 0) {
        headers.classed("hidden", false);
    } else {
        headers.classed("hidden", true);
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
             .fontSize(function(d) { return d.size; })
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
       .attr("transform", function(d) {
           return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
       })
       .text(function(d) { return d.text; });

    vis.transition()
       .duration(100)
       .style("font-size", function(d) { return d.size + "px"; })
       .style("font-family", "Impact")
       .style("fill", function(d, i) { return fill(i); })
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

function normalize(words) {
    var hiBound = findMax(words);
    for (var i=0; i<words.length; i++) {
        words[i]["size"] = words[i].value * (80 / hiBound);
    }
}
