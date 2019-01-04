# Bundle prepared data files for offline use
zipname = paste0('TN_RandomForest_Inputs_', g, "_", Sys.Date(), '.zip')

system(paste('zip', file.path('~/workingdata', zipname),
             file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_', grids[1], '.RData')),
             file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_', grids[1], '.csv')),
             file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_', grids[2], '.RData')),
             file.path(localdir, paste0(state, '_', do.months[1], '_to_', do.months[length(do.months)], '_', grids[2], '.csv'))
             ))

system(paste(
  'aws s3 cp',
  file.path('~/workingdata', zipname),
  file.path(teambucket, 'export_requests', zipname)
))
