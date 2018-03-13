this.ckan.module('validation-report', function (jQuery) {
  return {
    options: {
      report: null
    },
    initialize: function() {
      lintolReportingUI.render(
        lintolReportingUI.ReportView,
        {report: this.options.report},
        this.el[0]
      )
    }
  }
});
