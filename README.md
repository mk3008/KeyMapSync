# KeyMapSync

## Pattern

root | destination | mappable(*) | pattern
---- | ----------- | -------- | ------- 
T | T | T | default / insert only / integration
T | T | F | not support
T | F | T | not support
T | F | F | bridge
F | T | T | cascade
F | T | F | extension
F | F | T | not support
F | F | F | bridge

*has mappingtable, sync table, and sync version table. 

## default pattern
```
->datasource
  ->bridge
    ->destination
    ->map
    ->sync
    ->version
```
## cascade pattern
```
->datasource
  ->bridge
    ->destination
    ->map
    ->sync
    ->version
    ->cascade bridge1
      ->destination
      ->map
      ->sync
      ->version
    ->cascade bridge2
      ->destination
      ->map
      ->sync
      ->version
...
    ->cascade bridgeN
      ->destination
      ->map
      ->sync
      ->version
```
## extension pattern
```
->datasource
  ->bridge
    ->destination
    ->map
    ->sync
    ->version
      ->extension1
      ->extension2
      ...
      ->extensionN      
```
## validate bridge pattern
```
->datasource
  ->bridge
    ->abnormal bridge
    ->normal bridge
      ->destination
      ->map
      ->sync
      ->version   
```
## offset bridge pattern
```
->datasource
  ->expect bridge
    ->offset bridge
      ->destination
      ->offsetmap
      ->sync
      ->version
      ->map[delete]
    ->lack bridge
      ->destination
      ->map
      ->sync
      ->version
     
```

    
