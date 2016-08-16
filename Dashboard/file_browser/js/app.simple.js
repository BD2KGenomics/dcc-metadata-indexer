/*jshint globalstrict:true */
/*global angular:true */
'use strict';


/* Application Module */
angular.module('multiselect', [
  'multiselect.controllers',
  'multiselect.service',
  'elasticjs.service'
]);

/* Controller Module */
angular.module('multiselect.controllers', [])
  .controller('SearchCtrl', function($scope, translator, ejsResource) {

    // point to your ElasticSearch server
    //var ejs = ejsResource('http://mzgephdfgnfskfpm.api.qbox.io');
    var ejs = ejsResource('http://ucsc-cgl.org:9200');
    var index = 'analysis_file_index';
    var type = 'meta';
    // the fields we want to facet on
    var facets = ['center_name', 'program', 'project', 'workflow', 'analysis_type', 'specimen_type', 'file_type'];

    // for storing selected filters
    // format will be {field: [term1, term2], field2: [term1, term2]}
    var filters = {};

    // add's or removes a filter term
    $scope.handleFilter = function (field, term) {
      if (!_.has(filters, field)) {
        filters[field] = [];
      }

      var termIdx = _.indexOf(filters[field], term);
      if (termIdx === -1) {
        // add the filter
        filters[field].push(term);
      } else {
        // remove the filter
        filters[field].splice(termIdx, 1);
        if (filters[field].length === 0) {
          delete filters[field];
        }
      }

      $scope.search()
    };

    // if a filter is applied or not
    $scope.hasFilter = function (field, term) {
      return (_.has(filters, field) && _.contains(filters[field], term));
    }

    // define our search function that will be called when a user
    // submits a search or selects a facet
    $scope.search = function() {
      // setup the request
      var request = ejs.Request()
        .indices(index)
        .types(type)
        .sort('id') // sort by document id
        .query(ejs.MatchAllQuery()); // match all documents

      // create the facets
      var facetObjs = _.map(facets, function (facetField) {
        return ejs.TermsFacet(facetField + 'Facet')
          .field(facetField)
          .allTerms(true);
      });

      // create the filters
      var filterObjs = _.map(filters, function (filterTerms, filterField) {
        return ejs.TermsFilter(filterField, filterTerms);
      });

      // apply the facets to the request
      // make sure to add any facet filters (to update counts)
      _.each(facetObjs, function (facetObj) {
        var facetFilters = _.filter(filterObjs, function (filterObj) {
          return facetObj.field() !== filterObj.field();
        });

        if (facetFilters.length === 1) {
          facetObj.facetFilter(facetFilters[0]);
        } else if (facetFilters.length > 1) {
          facetObj.facetFilter(ejs.BoolFilter().must(facetFilters));
        }

        request.facet(facetObj);
      });

      // apply search filters to the request
      if (filterObjs.length === 1) {
        request.filter(filterObjs[0]);
      } else if (filterObjs.length > 1) {
        request.filter(ejs.BoolFilter().must(filterObjs));
      }

      // execute the search
      $scope.restQry = translator(request._self());
      $scope.results = request.doSearch();
    };

    $scope.search();
  });

/* Service Module */
angular.module('multiselect.service', [])
  .factory('translator', function () {
    var RealTypeOf = function(v) {
      if (typeof(v) == "object") {
        if (v === null) return "null";
        if (v.constructor == [].constructor) return "array";
        if (v.constructor == (new Date()).constructor) return "date";
        if (v.constructor == (new RegExp()).constructor) return "regex";
        return "object";
      }
      return typeof(v);
    };

    var FormatJSON = function(oData, sIndent) {
      if (arguments.length < 2) {
        sIndent = "";
      }

      var sIndentStyle = "    ";
      var sDataType = RealTypeOf(oData);
      var sHTML = "";
      var iCount = 0;

      // open object
      if (sDataType == "array") {
        if (oData.length === 0) {
          return "[]";
        }
        sHTML = "[";
      } else {
        iCount = 0;
        _.each(oData, function() {
          iCount++;
          return;
        });
        if (iCount === 0) { // object is empty
          return "{}";
        }
        sHTML = "{";
      }

      // loop through items
      iCount = 0;
      _.each(oData, function(vValue, sKey) {
        if (iCount > 0) {
          sHTML += ",";
        }
        if (sDataType == "array") {
          sHTML += ("\n" + sIndent + sIndentStyle);
        } else {
          sHTML += ("\n" + sIndent + sIndentStyle + "\"" + sKey + "\"" + ": ");
        }

        // display relevant data type
        switch (RealTypeOf(vValue)) {
          case "array":
          case "object":
            sHTML += FormatJSON(vValue, (sIndent + sIndentStyle));
            break;
          case "boolean":
          case "number":
            sHTML += vValue.toString();
            break;
          case "null":
            sHTML += "null";
            break;
          case "string":
            sHTML += ("\"" + vValue + "\"");
            break;
          default:
            sHTML += ("TYPEOF: " + typeof(vValue));
        }

        // loop
        iCount++;
      });

      // close object
      if (sDataType == "array") {
        sHTML += ("\n" + sIndent + "]");
      } else {
        sHTML += ("\n" + sIndent + "}");
      }

      // return
      return sHTML;
    };

    return FormatJSON;
  });
