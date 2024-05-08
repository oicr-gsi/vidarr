Fix calculated workflow output directory to be unique
* Directory where workflows calculated used to be a single directory `output`
* Changed so that directory has unique identifier to avoid file already exists errors with 
  multiple tests