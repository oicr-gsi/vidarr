Reprovisioning now creates new workflow run metadata and appends it to the original metadata.
The structure is as follows:
{ "submission": {(the original metadata)},
  "reprovision": [{(metadata created for the first reprovision)}, {(second reprovision)}, {etc}]
}
The new metadata reassigns all external keys as MANUAL assignments even if the original
metadata used ALL or REMAINING rules. For this reason, the original metadata is preserved in the
'submission' block.
This change fixes an issue with file checksums getting conflated or dropped during reprovisioning.