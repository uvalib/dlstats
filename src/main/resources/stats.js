window.onload = function() {

    var yearDivs = document.getElementsByClassName("year");
    for (var i = 0; i < yearDivs.length; i ++) {
        var year = yearDivs[i].getAttribute("id").substring(5);
        var button = document.createElement("button");
        button.setAttribute("type", "button");
        button.appendChild(document.createTextNode(year));
        button.addEventListener("click", selectYear);
        yearDivs[i].appendChild(button);
    }

    function selectYear() {
        var inputs = document.getElementsByTagName('input');
        for (var i = 0; i < inputs.length; i ++) {
            var input = inputs[i];
            if (input.getAttribute("type") == "checkbox") {
                var checkbox = input;
                if (checkbox.disabled == false && checkbox.getAttribute("value").indexOf(this.innerText) == 0) {
                    checkbox.checked = true;
                }
            }
        }

    }


}