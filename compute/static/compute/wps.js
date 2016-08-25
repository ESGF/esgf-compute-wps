$(document).ready(function() {
  $('#getcapabilities_btn').click(function() {
    $.get('/wps/?version=1.0.0&service=wps&request=getcapabilities', function(data) {
      $('#result').text(data);
    }, 'text');
  });

  $('#describeprocess_btn').click(function() {
    var url = '/wps/?version=1.0.0&service=wps&request=describeprocess&identifier=';
    var selected_process = $('#process_select option:selected').attr('value');

    $.get(url.concat(selected_process), function(data) {
      $('#result').text(data);
    }, 'text');
  });

  $('#processes').ready(function() {
    $.get('/api/processes', function(data) {
      var li = $('#processes');
      var select = $('#process_select');

      for (var i = 0; i < data.processes.length; i++) {
        var process = $('<li></li>');
        var details = $('<div></div>');
        var option = $('<option></option>');

        details.append(data.processes[i].Title);
        details.append('<br>');
        details.append(data.processes[i].Abstract);

        process.append(data.processes[i].Identifier);
        process.append(details);

        li.append(process);

        option.text(data.processes[i].Title);
        option.attr('value', data.processes[i].Identifier);

        select.append(option);
      }
    });
  });
});
