[string[]]$items = Get-ChildItem .\tests\ -Filter *.ts | ForEach-Object {
    return $_.Name
}
if ($items.Length -eq 0) {
    return;
}

$target = $items[0]
if ($items.Length -gt 1) {
    $target = gum filter $items
}
if ([string]::IsNullOrEmpty($target)) {
    return;
}

deno --sloppy-imports ./tests/$target
