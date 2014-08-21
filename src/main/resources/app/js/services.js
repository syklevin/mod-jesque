'use strict';


var pasiServices = angular.module('pasiServices', [])


pasiServices.factory('appSettings', [
  function(){

  }
]);

pasiServices.factory('authService', [
  function($http){
    return {

      'login': function(username, password){
        return $http.get('/login')
          .success(function(result){

          })
          .fail(function(error){

          })
      },
      'logout': function(){

      }

    };
  }
]);

pasiServices.factory('pasiClient', [
  function(){

    var conn = LvsConnection({ url: '/lvs', sessionId: sessionId });
    var lobbyHub = conn.createHub('lobby');
    var roomHub = conn.createHub('room');


    return {
      on: function(event, callback){

      }

    };
}]);
