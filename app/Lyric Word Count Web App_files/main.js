"use strict";

const BACKEND_END_POINT = "http://50.23.83.242:80/get_top_words_by_artist";

// To use for scaling n-gram counts so visualization is within bounds
function normalize(min, max) {
  return function (val) {
    return (val - min) / (max - min);
  }
}

$(function() {
    $("#search-btn").on("click", function() {

        // Remove any word clouds already present
        var wordClouds = document.getElementById("word-cloud");
        while (wordClouds.firstChild) {
          wordClouds.removeChild(wordClouds.firstChild);
        }

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
                $("#output-box").html(JSON.stringify(result));

                var sample_res = result;
                // d3.select("body").append("p").text(JSON.stringify(sample_res));
                for(var key in sample_res) {
                  if (key == "count") {
                    //d3.select("body").append("p").text(key + " : " + JSON.stringify(sample_res[key]));
                    for(var counts in sample_res[key]) {
                      //d3.select("body").append("p").text(counts + " : " + JSON.stringify(sample_res[key][counts]));
                      var ngram_words =[], ngram_counts = []; // In case we want to do something with just the arrays
                      for(var curr_word in sample_res[key][counts]) {
                        ngram_words.push(curr_word);
                        ngram_counts.push(parseInt(sample_res[key][counts][curr_word]));
                        //d3.select("body").append("p").text(curr_word + " : " + JSON.stringify(parseInt(sample_res[key][counts][curr_word])));
                      }

                      var normalized_counts = ngram_counts.map(normalize(Math.min(...ngram_counts), Math.max(...ngram_counts)));
                      var curr_data = Object.assign({}, ...ngram_words.map((n, index) => ({[n]: normalized_counts[index]})));
                      console.log(curr_data);

                      var w = window.innerWidth*0.5;
                      var h = window.innerHeight*0.75;
                      var p = 0;
                      if (curr_data.length < 10) {
                        p = 10;
                      }
                      var fill = d3.scale.category20();
                        d3.layout.cloud().size([w, h])
                            .words(ngram_words
                              // .map(function(d) {return {text: d, size: 10 + Math.random() * 90};
                              .map(function(d) {return {text: d, size: 25 + curr_data[d] * 45};
                            }))
                            .rotate(function() { return ~~(Math.random() * 2) * 90; })
                            .font("Impact")
                            .fontSize(function(d) { return d.size; })
                            .on("end", draw)
                            .start();
                        function draw(words) {
                          d3.select("#word-cloud").append("svg")
                              .attr("width", w)
                              .attr("height", h)
                            .append("g")
                              //.attr("transform", "translate(250,250)")
                              .attr("transform", function(d) {return "translate(" + w*0.5 + "," + h*0.5 + ")";})
                            .selectAll("text")
                              .data(words)
                            .enter().append("text")
                              .style("font-size", function(d) { return d.size + "px"; })
                              .style("font-family", "Impact")
                              .style("fill", function(d, i) { return fill(i); })
                              .attr("text-anchor", "middle")
                              .attr("transform", function(d) {
                                return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
                              })
                              .text(function(d) { return d.text; });
                        }
                      //d3.select("body").append("p").text(" ... ");
                    }
                  }
                }

            },
            error: function(){
                $("#output-box").html("NOTE: \'" + artistName + "\' NOT FOUND!<br /><br /><br />Here are some sample data instead:");

                //SAMPLE FOR NOW
                var sample_res = {"count":{"1-gram": {"love":"400", "hate":"100","candy":"300","apple":"17","cars":"89","world":"75"},
                                           "2-gram": {"party time":"70", "little hats":"90", "fast lane":"10", "beautiful day":"61", "perfectly aligned":"22"},
                                           "3-gram": {"in the end":"3", "such great heights":"18", "forget about me":"5"}},
                                  "tfidf":{ "1-gram": {"love":"1", "hate":"1"},"2-gram": {"party time":"1", "little hats":"1"},"3-gram": {"in the end":"1", "i like big":"1"}}};
                // d3.select("body").append("p").text(JSON.stringify(sample_res));
                for(var key in sample_res) {
                  if (key == "count") {
                    //d3.select("body").append("p").text(key + " : " + JSON.stringify(sample_res[key]));
                    for(var counts in sample_res[key]) {
                      //d3.select("body").append("p").text(counts + " : " + JSON.stringify(sample_res[key][counts]));
                      var ngram_words =[], ngram_counts = []; // In case we want to do something with just the arrays
                      for(var curr_word in sample_res[key][counts]) {
                        ngram_words.push(curr_word);
                        ngram_counts.push(parseInt(sample_res[key][counts][curr_word]));
                        //d3.select("body").append("p").text(curr_word + " : " + JSON.stringify(parseInt(sample_res[key][counts][curr_word])));
                      }

                      var normalized_counts = ngram_counts.map(normalize(Math.min(...ngram_counts), Math.max(...ngram_counts)));
                      var curr_data = Object.assign({}, ...ngram_words.map((n, index) => ({[n]: normalized_counts[index]})));
                      console.log(curr_data);

                      var w = window.innerWidth*0.5;
                      var h = window.innerHeight*0.75;
                      var p = 0;
                      if (curr_data.length < 10) {
                        p = 10;
                      }

                      var fill = d3.scale.category20();
                        d3.layout.cloud().size([w, h])
                            .words(ngram_words
                              // .map(function(d) {return {text: d, size: 10 + Math.random() * 90};
                              .map(function(d) {return {text: d, size: 25 + curr_data[d] * 45};
                              //.map(function(d) {return {text: d, size: w*0.02 + curr_data[d] * w*0.18};
                            }))
                            .rotate(function() { return ~~(Math.random() * 2) * 90; })
                            .font("Impact")
                            .fontSize(function(d) { return d.size; })
                            .on("end", draw)
                            .start();
                        function draw(words) {
                          d3.select("#word-cloud").append("svg")
                              .attr("width", w)
                              .attr("height", h)
                            .append("g")
                              //.attr("transform", "translate(250,250)")
                              .attr("transform", function(d) {return "translate(" + w*0.5 + "," + h*0.5 + ")";})
                            .selectAll("text")
                              .data(words)
                            .enter().append("text")
                              .style("font-size", function(d) { return d.size + "px"; })
                              .style("font-family", "Impact")
                              .style("fill", function(d, i) { return fill(i); })
                              .attr("text-anchor", "middle")
                              .attr("transform", function(d) {
                                return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
                              })
                              .text(function(d) { return d.text; });
                        }
                      //d3.select("body").append("p").text(" ... ");
                    }

                  }

                }

                // var fill = d3.scale.category20();
                //   d3.layout.cloud().size([300, 300])
                //       .words([
                //         "Hello", "world", "normally", "you", "want", "more", "words",
                //         "than", "this", artistName, artistName, artistName, artistName]
                //         .map(function(d) {return {text: d, size: 10 + Math.random() * 90};
                //       }))
                //       .rotate(function() { return ~~(Math.random() * 2) * 90; })
                //       .font("Impact")
                //       .fontSize(function(d) { return d.size; })
                //       .on("end", draw)
                //       .start();
                //   function draw(words) {
                //     d3.select("#word-cloud").append("svg")
                //         .attr("width", 300)
                //         .attr("height", 300)
                //       .append("g")
                //         .attr("transform", "translate(150,150)")
                //       .selectAll("text")
                //         .data(words)
                //       .enter().append("text")
                //         .style("font-size", function(d) { return d.size + "px"; })
                //         .style("font-family", "Impact")
                //         .style("fill", function(d, i) { return fill(i); })
                //         .attr("text-anchor", "middle")
                //         .attr("transform", function(d) {
                //           return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
                //         })
                //         .text(function(d) { return d.text; });
                //   }
            }
        });
    });
});
