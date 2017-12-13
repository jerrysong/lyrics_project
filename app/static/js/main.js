"use strict";

const BACKEND_END_POINT = "http://50.23.83.242:80/get_top_words_by_artist";

$(function() {
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
                $("#output-box").html(JSON.stringify(result));
            },
            error: function(){
                $("#output-box").html("");
            }
        });
    });
});
